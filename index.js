import 'dotenv/config';
import express from 'express';
import { Firestore } from '@google-cloud/firestore';
import { BigQuery } from '@google-cloud/bigquery';

const app = express();
const PORT = process.env.PORT || 8080;

// Initialize clients
// On GCP, they will automatically use the service account.
// Locally, they will use your `gcloud auth application-default login` credentials.
const firestore = new Firestore();
const bigquery = new BigQuery();

app.get('/test-connections', async (req, res) => {
  try {
    // 1. Test Firestore Connection
    console.log('Testing Firestore connection...');
    const usersSnapshot = await firestore.collection('users').limit(1).get();
    const firestoreResult = `Successfully fetched ${usersSnapshot.size} user(s) from Firestore.`;
    console.log(firestoreResult);

    // 2. Test BigQuery Connection
    // !!! IMPORTANT: Replace with your actual dataset and table ID !!!
    const query = 'SELECT * FROM `referrals-470107.Scraper_DS.jobs2` LIMIT 1';
    console.log('Testing BigQuery connection with query:', query);
    const [rows] = await bigquery.query({ query });
    const bigqueryResult = `Successfully fetched ${rows.length} row(s) from BigQuery.`;
    console.log(bigqueryResult);

    // 3. Send success response
    res.status(200).send({
      message: '✅ All connections successful!',
      firestore: firestoreResult,
      bigquery: bigqueryResult
    });

  } catch (error) {
    console.error('Connection test failed:', error);
    res.status(500).send({
      message: '🔥 Connection test failed.',
      error: error.message
    });
  }
});

export { app }; // <-- Add this line