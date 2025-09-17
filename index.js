import express from 'express';
import { Firestore } from '@google-cloud/firestore';
import { BigQuery } from '@google-cloud/bigquery';

const app = express();
const firestore = new Firestore();
const bigquery = new BigQuery();

/**
 * MASTER MATCHING QUERY - Job-to-User Matching Algorithm
 * 
 * This complex SQL query matches users with suitable job opportunities using a multi-step approach:
 * 1. UserPreferences CTE: Extracts user preferences from Firestore data
 * 2. PotentialMatches CTE: Applies matching logic based on experience, location, domain, and leadership
 * 3. MatchesWithHistory CTE: Adds notification history to prevent spam
 * 4. Final SELECT: Applies timing rules for notifications
 * 
 * See QUERY_EXPLANATION.md for detailed documentation.
 */
const MASTER_MATCHING_QUERY = `
-- CTE 1: Extract and normalize user preferences from Firestore export
WITH UserPreferences AS (
  SELECT
    document_id AS user_id,
    JSON_EXTRACT_SCALAR(data, '$.preferences.status') AS status,                    -- User's job status preference
    CAST(JSON_EXTRACT_SCALAR(data, '$.preferences.minExperience') AS INT64) AS min_experience,
    CAST(JSON_EXTRACT_SCALAR(data, '$.preferences.maxExperience') AS INT64) AS max_experience,
    TRIM(JSON_EXTRACT_SCALAR(data, '$.preferences.degree'), '"') AS degree,        -- Required for student positions
    TRIM(JSON_EXTRACT_SCALAR(data, '$.preferences.managementInterest'), '"') AS management_interest,
    JSON_EXTRACT_ARRAY(data, '$.preferences.managementLevel') AS management_level,  -- Array of acceptable management levels
    JSON_EXTRACT_ARRAY(data, '$.preferences.domains') AS domains,                  -- Array of preferred job domains
    JSON_EXTRACT_ARRAY(data, '$.preferences.locations') AS locations               -- Array of preferred locations
  FROM \`referrals-470107.firestore_export.users_raw_latest\`
  WHERE JSON_EXTRACT_SCALAR(data, '$.state') = 'completed'                         -- Only users with completed profiles
),

-- CTE 2: Core matching logic - cross join users with jobs and apply filtering criteria
PotentialMatches AS (
  SELECT
    users.user_id,
    users.status,
    jobs.job_id,
    jobs.job_title,
    jobs.job_url
  FROM UserPreferences AS users
  CROSS JOIN \`referrals-470107.matching.clean_jobs_view\` AS jobs
  WHERE
    -- FILTER 1: Experience/Status Matching Logic
    (
      -- Student positions: Must be student job AND user's degree must be in required degrees
      (users.status = 'student_position' AND jobs.is_student_job = TRUE AND users.degree IN UNNEST(IFNULL(jobs.required_degrees, []))) OR
      -- No experience: Job must accept 0 years experience
      (users.status = 'no_experience_position' AND 0 IN UNNEST(IFNULL(jobs.experience_levels, []))) OR
      -- Experienced: Job's experience range must overlap with user's range
      (users.status = 'experience_position' AND EXISTS (SELECT 1 FROM UNNEST(IFNULL(jobs.experience_levels, [])) AS lvl WHERE lvl BETWEEN users.min_experience AND users.max_experience))
    )
    
    -- FILTER 2: Location Matching Logic
    AND (
      -- Match if any user location is in job locations, OR either side supports remote work
      EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.locations, [])) AS user_loc 
              WHERE TRIM(user_loc, '"') IN UNNEST(IFNULL(jobs.locations, [])) 
                 OR TRIM(user_loc, '"') = 'remote' 
                 OR 'remote' IN UNNEST(IFNULL(jobs.locations, [])))
    )
    
    -- FILTER 3: Domain Matching Logic
    AND (
      -- If user has no domain preferences, match all jobs
      users.domains IS NULL OR ARRAY_LENGTH(users.domains) = 0 OR 
      -- Otherwise, match if any user domain exists in job domains
      EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.domains, [])) AS user_domain 
              WHERE TRIM(user_domain, '"') IN UNNEST(IFNULL(jobs.domains, [])))
    )
    
    -- FILTER 4: Leadership/Management Matching Logic
    AND (
      CASE
        -- Non-experienced users: Only jobs without leadership requirements
        WHEN users.status != 'experience_position' THEN ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) = 0
        -- User wants no management: Only jobs without leadership requirements
        WHEN users.management_interest = 'no_management' THEN ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) = 0
        -- User wants management only: Job must require leadership AND user's level must match
        WHEN users.management_interest = 'management_only' THEN (
          ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) > 0 AND 
          EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.management_level, [])) AS user_level 
                  WHERE TRIM(user_level, '"') IN UNNEST(IFNULL(jobs.leadership_levels, [])))
        )
        -- User wants both management and individual: Job can have no leadership OR user's level must match
        WHEN users.management_interest = 'management_and_individual' THEN (
          ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) = 0 OR 
          EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.management_level, [])) AS user_level 
                  WHERE TRIM(user_level, '"') IN UNNEST(IFNULL(jobs.leadership_levels, [])))
        )
        ELSE TRUE  -- Default case: allow all matches
      END
    )
),

-- CTE 3: Add notification history to prevent duplicate notifications
MatchesWithHistory AS (
    SELECT
        pm.user_id,
        pm.job_id,
        pm.job_title,
        pm.job_url,
        pm.status,
        MAX(hist.sent_timestamp) as last_sent  -- Get most recent notification timestamp for this user-job pair
    FROM PotentialMatches pm
    LEFT JOIN \`referrals-470107.matching.match_history\` hist
        ON pm.user_id = hist.user_id AND pm.job_id = CAST(hist.job_id AS STRING)  -- Join on user and job
    GROUP BY 1, 2, 3, 4, 5  -- Group to get max timestamp per match
)

-- Final SELECT: Apply notification timing rules based on user status
SELECT
    user_id,
    CAST(job_id AS STRING) AS job_id,
    job_title,
    job_url
FROM MatchesWithHistory
WHERE
    last_sent IS NULL OR  -- Never been notified about this job
    (
        -- Students and no-experience users: notify again after 3+ days
        (status IN ('student_position', 'no_experience_position') AND DATE_DIFF(CURRENT_DATE(), DATE(last_sent), DAY) > 3) OR
        -- Experienced users: notify again after 14+ days (less frequent)
        (status = 'experience_position' AND DATE_DIFF(CURRENT_DATE(), DATE(last_sent), DAY) > 14)
    )
`;

/**
 * Main endpoint triggered by Cloud Scheduler for job matching process.
 * 
 * Process flow:
 * 1. Execute MASTER_MATCHING_QUERY to find new user-job matches
 * 2. If matches found, write them to Firestore for user access
 * 3. Record notification history in BigQuery to prevent duplicates
 * 4. Return success/failure status
 * 
 * The query handles all the complex matching logic, this endpoint just
 * orchestrates the data flow between BigQuery and Firestore.
 */
app.post('/run', async (req, res) => {
  try {
    console.log('Starting job matching process...');

    const [jobsToSync] = await bigquery.query({ query: MASTER_MATCHING_QUERY });
    
    if (jobsToSync.length === 0) {
      console.log('No new matches found to sync.');
      res.status(200).send('No new matches found.');
      return;
    }

    console.log(`Found ${jobsToSync.length} new matches to sync.`);

    const firestoreWrites = [];
    const historyRows = [];
    const now = new Date();

    for (const job of jobsToSync) {
      const { user_id, job_id, job_title, job_url } = job;
      
      const matchRef = firestore.collection('users').doc(user_id).collection('matchedJobs').doc(job_id);
      firestoreWrites.push(matchRef.set({
        status: 'new',
        title: job_title,
        url: job_url,
        matchedTimestamp: now
      }, { merge: true }));

      // --- FIX: Convert job_id back to a number for BigQuery ---
      historyRows.push({
        user_id: user_id,
        job_id: parseInt(job_id, 10),
        sent_timestamp: now.toISOString()
      });
    }

    await Promise.all([
      Promise.all(firestoreWrites),
      bigquery.dataset('matching').table('match_history').insert(historyRows)
    ]);
    
    const resultMessage = `Successfully synced ${jobsToSync.length} matches.`;
    console.log(resultMessage);
    res.status(200).send(resultMessage);

  } catch (error) {
    console.error('Job matching process failed:', error);
    res.status(500).send(`Error: ${error.message}`);
  }
});

export { app };