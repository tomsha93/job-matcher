import express from 'express';
import { Firestore } from '@google-cloud/firestore';
import { BigQuery } from '@google-cloud/bigquery';

const app = express();
const firestore = new Firestore();
const bigquery = new BigQuery();

// This is the Master Matching Query with the corrected SQL syntax.
const MASTER_MATCHING_QUERY = `
-- CTE 1: Extract and normalize user preferences from Firestore export
WITH UserPreferences AS (
  SELECT
    document_id AS user_id,
    JSON_EXTRACT_SCALAR(data, '$.preferences.status') AS status,                     -- User's job status preference

    -- Normalize min/max experience (handles swapped inputs; leaves NULLs as NULLs)
    LEAST(
      CAST(JSON_EXTRACT_SCALAR(data, '$.preferences.minExperience') AS INT64),
      CAST(JSON_EXTRACT_SCALAR(data, '$.preferences.maxExperience') AS INT64)
    ) AS min_experience,
    GREATEST(
      CAST(JSON_EXTRACT_SCALAR(data, '$.preferences.minExperience') AS INT64),
      CAST(JSON_EXTRACT_SCALAR(data, '$.preferences.maxExperience') AS INT64)
    ) AS max_experience,

    TRIM(JSON_EXTRACT_SCALAR(data, '$.preferences.degree'), '"') AS degree,          -- Required for student positions
    TRIM(JSON_EXTRACT_SCALAR(data, '$.preferences.managementInterest'), '"') AS management_interest,
    JSON_EXTRACT_ARRAY(data, '$.preferences.managementLevel') AS management_level,   -- Array of acceptable management levels
    JSON_EXTRACT_ARRAY(data, '$.preferences.domains') AS domains,                   -- Array of preferred job domains
    JSON_EXTRACT_ARRAY(data, '$.preferences.locations') AS locations                -- Array of preferred locations
  FROM \`referrals-470107.firestore_export.users_raw_latest\`
  WHERE JSON_EXTRACT_SCALAR(data, '$.state') = 'completed'                           -- Only users with completed profiles
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
      (users.status = 'student_position'
       AND jobs.is_student_job = TRUE
       AND users.degree IN UNNEST(IFNULL(jobs.required_degrees, [])))

      OR

      -- No experience: Job must explicitly accept 0 years (experience_levels is an array of discrete acceptable values)
      (users.status = 'no_experience_position'
       AND 0 IN UNNEST(IFNULL(jobs.experience_levels, [])))

      OR

      -- Experienced: experience_levels is an ARRAY of discrete acceptable integers.
      -- Match if ANY listed job level falls within the user's [min_experience, max_experience] range.
      (users.status = 'experience_position'
       AND EXISTS (
         SELECT 1
         FROM UNNEST(IFNULL(jobs.experience_levels, [])) AS job_level
         WHERE SAFE_CAST(job_level AS INT64)
               BETWEEN COALESCE(users.min_experience, 0)   -- open lower bound if NULL
                   AND COALESCE(users.max_experience, 100) -- open upper bound if NULL
       ))
    )

    -- FILTER 2: Location Matching Logic
    AND (
      -- Match if any user location is in job locations, OR either side supports remote work
      EXISTS (
        SELECT 1
        FROM UNNEST(IFNULL(users.locations, [])) AS user_loc
        WHERE TRIM(user_loc, '"') IN UNNEST(IFNULL(jobs.locations, []))
           OR TRIM(user_loc, '"') = 'remote'
           OR 'remote' IN UNNEST(IFNULL(jobs.locations, []))
      )
    )

    -- FILTER 3: Domain Matching Logic
    AND (
      -- If user has no domain preferences, match all jobs
      users.domains IS NULL OR ARRAY_LENGTH(users.domains) = 0
      -- Otherwise, match if any user domain exists in job domains
      OR EXISTS (
        SELECT 1
        FROM UNNEST(IFNULL(users.domains, [])) AS user_domain
        WHERE TRIM(user_domain, '"') IN UNNEST(IFNULL(jobs.domains, []))
      )
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
          ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) > 0
          AND EXISTS (
            SELECT 1
            FROM UNNEST(IFNULL(users.management_level, [])) AS user_level
            WHERE TRIM(user_level, '"') IN UNNEST(IFNULL(jobs.leadership_levels, []))
          )
        )

        -- User wants both management and individual: Job can have no leadership OR user's level must match
        WHEN users.management_interest = 'management_and_individual' THEN (
          ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) = 0
          OR EXISTS (
            SELECT 1
            FROM UNNEST(IFNULL(users.management_level, [])) AS user_level
            WHERE TRIM(user_level, '"') IN UNNEST(IFNULL(jobs.leadership_levels, []))
          )
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
    MAX(hist.sent_timestamp) AS last_sent  -- Most recent notification timestamp for this user-job pair
  FROM PotentialMatches pm
  LEFT JOIN \`referrals-470107.matching.match_history\` AS hist
    ON pm.user_id = hist.user_id
   AND pm.job_id = CAST(hist.job_id AS STRING)
  GROUP BY 1, 2, 3, 4, 5
)

-- Final SELECT: Apply notification timing rules based on user status
SELECT
  user_id,
  job_id, -- Already a string from clean_jobs_view
  job_title,
  job_url,
  status
FROM MatchesWithHistory
WHERE
  last_sent IS NULL  -- Never been notified about this job
  OR (
       -- Students and no-experience users: notify again after 3+ days
       (status IN ('student_position', 'no_experience_position')
        AND DATE_DIFF(CURRENT_DATE(), DATE(last_sent), DAY) > 3)
       -- Experienced users: notify again after 14+ days (less frequent)
       OR (status = 'experience_position'
           AND DATE_DIFF(CURRENT_DATE(), DATE(last_sent), DAY) > 14)
     );
`;

// This is the main endpoint that Cloud Scheduler will trigger.
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

      // --- FIX: No longer need to parse to int ---
      historyRows.push({
        user_id: user_id,
        job_id: job_id, // job_id is now correctly a string
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