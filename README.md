# Job Matcher Service

A service to match users with suitable job opportunities based on their preferences and requirements.

## Overview

This service is deployed as a Google Cloud Function that:
1. Extracts user preferences from Firestore
2. Matches users with jobs from BigQuery using complex filtering logic
3. Stores matches in Firestore for user access
4. Tracks notification history to prevent spam

## Key Files

- `index.js` - Main Express.js application with the matching endpoint
- `matchingLogic.js` - Business logic for matching users to jobs (alternative approach)
- `constants.js` - Job domain and category definitions
- `QUERY_EXPLANATION.md` - **Detailed explanation of the SQL matching query**

## Query Documentation

The core of this service is a complex BigQuery SQL query that handles the matching logic. For a comprehensive explanation of how the query works, see [QUERY_EXPLANATION.md](./QUERY_EXPLANATION.md).

## Deployment

This service is deployed to Google Cloud Functions using Cloud Build:

```bash
gcloud functions deploy job-matcher --runtime nodejs18 --trigger=http
```

## Usage

The service exposes a single endpoint:
- `POST /run` - Triggered by Cloud Scheduler to find and process new job matches