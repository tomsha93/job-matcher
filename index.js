import express from 'express';
import { Firestore } from '@google-cloud/firestore';
import { BigQuery } from '@google-cloud/bigquery';
import { isMatch } from './matchingLogic.js';

const app = express();
const PORT = process.env.PORT || 8080;

const firestore = new Firestore();
const bigquery = new BigQuery();

// This is the main endpoint that Cloud Scheduler will trigger.
app.get('/run', async (req, res) => {
  try {
    console.log('Starting job matching process...');

    // 1. Fetch all users who have completed onboarding
    const usersSnapshot = await firestore.collection('users').where('state', '==', 'completed').get();
    const users = [];
    usersSnapshot.forEach(doc => {
        users.push({ id: doc.id, preferences: doc.data().preferences });
    });
    console.log(`Found ${users.length} completed users.`);

    // 2. Fetch all jobs from BigQuery
    // !!! IMPORTANT: Replace with your actual dataset and table ID !!!
    const query = 'SELECT * FROM `referrals-470107.Scraper_DS.jobs2`';
    const [jobs] = await bigquery.query({ query });
    console.log(`Found ${jobs.length} jobs.`);

    let matchCount = 0;

    // 3. Loop and find matches
    for (const user of users) {
        for (const job of jobs) {
            if (isMatch(user.preferences, job)) {
                console.log(`Match found! User: ${user.id}, Job: ${job.title}`);
                matchCount++;
                // TODO: In the next step, we will add the code here to save the match to Firestore.
            }
        }
    }

    const resultMessage = `Matching process completed. Found ${matchCount} total potential matches.`;
    console.log(resultMessage);
    res.status(200).send(resultMessage);

  } catch (error) {
    console.error('Job matching process failed:', error);
    res.status(500).send(`Error: ${error.message}`);
  }
});

// The Functions Framework will handle starting the server.
export { app };