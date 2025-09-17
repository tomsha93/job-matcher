# BigQuery Job Matching Query Explanation

This document explains how the `MASTER_MATCHING_QUERY` in `index.js` works. This complex SQL query is designed to match users with suitable job opportunities based on their preferences and requirements.

## Overview

The query uses Common Table Expressions (CTEs) to break down the matching logic into manageable steps:

1. **UserPreferences CTE**: Extracts and normalizes user preferences from Firestore data
2. **PotentialMatches CTE**: Performs the core matching logic between users and jobs
3. **MatchesWithHistory CTE**: Adds historical data to prevent duplicate notifications
4. **Final SELECT**: Filters results based on notification timing rules

## Detailed Breakdown

### 1. UserPreferences CTE

```sql
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
  FROM `referrals-470107.firestore_export.users_raw_latest`
  WHERE JSON_EXTRACT_SCALAR(data, '$.state') = 'completed'
)
```

**Purpose**: Extracts user preferences from Firestore export data and transforms them into a structured format.

**Key Operations**:
- Extracts JSON fields from Firestore document data
- Converts experience values to integers
- Removes quotes from string fields using TRIM
- Extracts arrays for complex fields (domains, locations, management levels)
- Filters only users with completed profiles

### 2. PotentialMatches CTE

This is the core matching logic that performs a CROSS JOIN between users and jobs, then applies multiple filtering conditions:

```sql
PotentialMatches AS (
  SELECT
    users.user_id,
    users.status,
    jobs.job_id,
    jobs.job_title,
    jobs.job_url
  FROM UserPreferences AS users
  CROSS JOIN `referrals-470107.matching.clean_jobs_view` AS jobs
  WHERE
    -- Experience/Status Matching
    (
      (users.status = 'student_position' AND jobs.is_student_job = TRUE AND users.degree IN UNNEST(IFNULL(jobs.required_degrees, []))) OR
      (users.status = 'no_experience_position' AND 0 IN UNNEST(IFNULL(jobs.experience_levels, []))) OR
      (users.status = 'experience_position' AND EXISTS (SELECT 1 FROM UNNEST(IFNULL(jobs.experience_levels, [])) AS lvl WHERE lvl BETWEEN users.min_experience AND users.max_experience))
    )
    -- Location Matching
    AND (
      EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.locations, [])) AS user_loc WHERE TRIM(user_loc, '"') IN UNNEST(IFNULL(jobs.locations, [])) OR TRIM(user_loc, '"') = 'remote' OR 'remote' IN UNNEST(IFNULL(jobs.locations, [])))
    )
    -- Domain Matching
    AND (
      users.domains IS NULL OR ARRAY_LENGTH(users.domains) = 0 OR EXISTS (SELECT 1 FROM UNNEST(IFNULL(users.domains, [])) AS user_domain WHERE TRIM(user_domain, '"') IN UNNEST(IFNULL(jobs.domains, [])))
    )
    -- Leadership/Management Matching
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
```

#### Experience/Status Matching Logic:
- **Student positions**: Matches if job is marked as student job AND user's degree is in required degrees
- **No experience positions**: Matches if job accepts 0 years of experience
- **Experience positions**: Matches if job's experience range overlaps with user's min/max experience

#### Location Matching Logic:
- Matches if any user location exists in job locations
- Always matches if either user wants remote work OR job offers remote work
- Uses UNNEST to handle array comparisons

#### Domain Matching Logic:
- If user has no domain preferences, matches all jobs
- Otherwise, matches if any user domain exists in job domains
- Uses EXISTS subquery for efficient array intersection

#### Leadership/Management Matching Logic:
- **Non-experience positions**: Only match jobs without leadership requirements
- **No management interest**: Only match jobs without leadership requirements
- **Management only**: Match jobs that require leadership AND user's management level matches
- **Management and individual**: Match jobs with no leadership OR user's management level matches

### 3. MatchesWithHistory CTE

```sql
MatchesWithHistory AS (
    SELECT
        pm.user_id,
        pm.job_id,
        pm.job_title,
        pm.job_url,
        pm.status,
        MAX(hist.sent_timestamp) as last_sent
    FROM PotentialMatches pm
    LEFT JOIN `referrals-470107.matching.match_history` hist
        ON pm.user_id = hist.user_id AND pm.job_id = CAST(hist.job_id AS STRING)
    GROUP BY 1, 2, 3, 4, 5
)
```

**Purpose**: Adds historical notification data to prevent spam and duplicate notifications.

**Key Operations**:
- LEFT JOIN with match history to get previous notification timestamps
- Uses MAX to get the most recent notification time for each user-job pair
- Converts job_id to string for consistent comparison

### 4. Final SELECT with Timing Logic

```sql
SELECT
    user_id,
    CAST(job_id AS STRING) AS job_id,
    job_title,
    job_url
FROM MatchesWithHistory
WHERE
    last_sent IS NULL OR
    (
        (status IN ('student_position', 'no_experience_position') AND DATE_DIFF(CURRENT_DATE(), DATE(last_sent), DAY) > 3) OR
        (status = 'experience_position' AND DATE_DIFF(CURRENT_DATE(), DATE(last_sent), DAY) > 14)
    )
```

**Purpose**: Controls notification frequency based on user status.

**Timing Rules**:
- **Never notified**: Always include (last_sent IS NULL)
- **Student/No experience**: Wait 3+ days between notifications
- **Experienced**: Wait 14+ days between notifications

## Data Flow Summary

1. **Input**: Firestore user data + BigQuery jobs data
2. **Processing**: 
   - Extract user preferences
   - Cross join users with all jobs
   - Apply multi-criteria filtering
   - Add notification history
   - Apply timing constraints
3. **Output**: List of user-job matches ready for notification

## Key Features

- **Scalable**: Uses BigQuery's distributed processing
- **Flexible**: Handles various user statuses and preferences
- **Smart**: Prevents notification spam with timing controls
- **Robust**: Uses IFNULL and defensive coding for array operations
- **Efficient**: Uses EXISTS and proper indexing strategies

## Usage in Application

The query result is consumed by the Express.js endpoint which:
1. Executes the query via BigQuery client
2. Writes matches to Firestore for user access
3. Records notification history to prevent duplicates
4. Returns success/failure status