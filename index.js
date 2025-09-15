import express from 'express';
import { Firestore } from '@google-cloud/firestore';
import { BigQuery } from '@google-cloud/bigquery';

const app = express();
const firestore = new Firestore();
const bigquery = new BigQuery();

// This is the Master Matching Query with the corrected SQL syntax.
const MASTER_MATCHING_QUERY = `
WITH UserPreferences AS (
  SELECT
    document_id AS user_id,
    JSON_EXTRACT_SCALAR(data, '$.preferences.status') AS status,
    CAST(JSON_EXTRACT_SCALAR(data, '$.preferences.minExperience') AS INT64) AS min_experience,
    CAST(JSON_EXTRACT_SCALAR(data, '$.preferences.maxExperience') AS INT64) AS max_experience,
    TRIM(JSON_EXTRACT_SCALAR(data, '$.preferences.degree'), '"') AS degree,
    TRIM(JSON_EXTRACT_SCALAR(data, '$.preferences.managementInterest'), '"') AS management_interest,
    JSON_EXTRACT_ARRAY(data, '$.preferences.managementLevel') AS management_level,
    JSON_EXTRACT_ARRAY(data, '$.preferences.domains') AS domains,
    JSON_EXTRACT_ARRAY(data, '$.preferences.locations') AS locations
  FROM \`referrals-470107.firestore_export.users_raw_latest\`
  WHERE JSON_EXTRACT_SCALAR(data, '$.state') = 'completed'
),
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
    (
      (users.status = 'student_position' AND jobs.is_student_job = TRUE AND users.degree IN UNNEST(IFNULL(jobs.required_degrees, []))) OR
      (users.status = 'no_experience_position' AND 0 IN UNNEST(IFNULL(jobs.experience_levels, []))) OR
      (users.status = 'experience_position' AND EXISTS (SELECT 1 FROM UNNEST(IFNULL(jobs.experience_levels, [])) AS lvl WHERE lvl BETWEEN users.min_experience AND users.max_experience))
    )
    AND (
      EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.locations, [])) AS user_loc WHERE TRIM(user_loc, '"') IN UNNEST(IFNULL(jobs.locations, [])) OR TRIM(user_loc, '"') = 'remote' OR 'remote' IN UNNEST(IFNULL(jobs.locations, [])))
    )
    AND (
      users.domains IS NULL OR ARRAY_LENGTH(users.domains) = 0 OR EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.domains, [])) AS user_domain WHERE TRIM(user_domain, '"') IN UNNEST(IFNULL(jobs.domains, [])))
    )
    AND (
      CASE
        WHEN users.status != 'experience_position' THEN ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) = 0
        WHEN users.management_interest = 'no_management' THEN ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) = 0
        WHEN users.management_interest = 'management_only' THEN (ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) > 0 AND EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.management_level, [])) AS user_level WHERE TRIM(user_level, '"') IN UNNEST(IFNULL(jobs.leadership_levels, []))))
        WHEN users.management_interest = 'management_and_individual' THEN (ARRAY_LENGTH(IFNULL(jobs.leadership_levels, [])) = 0 OR EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.management_level, [])) AS user_level WHERE TRIM(user_level, '"') IN UNNEST(IFNULL(jobs.leadership_levels, []))))
        ELSE TRUE
      END
    )
)
-- Final step: Filter the potential matches against the send history
SELECT
    pm.user_id,
    pm.job_id,
    pm.job_title,
    pm.job_url,
    pm.status
FROM
    PotentialMatches pm
LEFT JOIN
    \`referrals-470107.matching.match_history\` hist
ON
    pm.user_id = hist.user_id AND pm.job_id = hist.job_id
GROUP BY
    pm.user_id,
    pm.job_id,
    pm.job_title,
    pm.job_url,
    pm.status
HAVING
    -- Condition 1: The job has never been sent (the MAX timestamp will be NULL)
    MAX(hist.sent_timestamp) IS NULL
    OR
    -- Condition 2: The job was sent, but enough time has passed to resend it
    (
        (pm.status IN ('student_position', 'no_experience_position') AND DATE_DIFF(CURRENT_DATE(), DATE(MAX(hist.sent_timestamp)), DAY) > 3)
        OR
        (pm.status = 'experience_position' AND DATE_DIFF(CURRENT_DATE(), DATE(MAX(hist.sent_timestamp)), DAY) > 14)
    )
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
      
      const matchRef = firestore.collection('users').doc(user_id).collection('matchedJobs').doc(String(job_id));
      firestoreWrites.push(matchRef.set({
        status: 'new',
        title: job_title,
        url: job_url,
        matchedTimestamp: now
      }, { merge: true }));

      historyRows.push({
        user_id: user_id,
        job_id: job_id,
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