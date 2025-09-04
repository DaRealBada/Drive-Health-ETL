const express = require('express');
// We only need the main BigQuery client
const { BigQuery } = require('@google-cloud/bigquery');
// NEW: Import the built-in crypto library for hashing.
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 8080;

// We only need the main BigQuery client instance
const bigquery = new BigQuery();

// Configuration from environment variables
const DATASET_ID = process.env.DATASET_ID || 'call_audits';
const TABLE_ID = process.env.TABLE_ID || 'processed_calls';
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE) || 0.05;

app.use(express.json());

// Health check endpoint
app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

// Single-message processing endpoint, triggered by Pub/Sub Push
app.post('/process-call', async (req, res) => {
  try {
    const hashData = (data) => {
      const piiFields = ['caller', 'receiver'];
      const hashedData = { ...data };
      for (const field of piiFields) {
        if (hashedData[field]) {
          const hash = crypto.createHash('sha256');
          hash.update(hashedData[field]);
          hashedData[field] = hash.digest('hex');
        }
      }
      return hashedData;
    };

    const message = req.body.message;
    if (!message || !message.data) {
      console.warn('Invalid Pub/Sub message format.');
      return res.status(400).send('Bad Request');
    }
    const callData = JSON.parse(Buffer.from(message.data, 'base64').toString());

    if (!callData.call_id) {
      console.warn('Message missing call_id, acknowledging to avoid retries.');
      return res.status(200).send('Message acknowledged.'); 
    }

    if (Math.random() < AUDIT_RATE) {
      console.log(`Call ${callData.call_id} SELECTED for audit`);
      
      const hashedCallData = hashData(callData);

      const rowToInsert = {
        call_id: callData.call_id,
        timestamp: callData.timestamp,
        duration: callData.duration,
        flagged_for_audit: true,
        processed_at: new Date().toISOString(),
        original_metadata: JSON.stringify(hashedCallData)
      };

      // UPDATED: Use the simple and reliable table.insert() method
      await bigquery
        .dataset(DATASET_ID)
        .table(TABLE_ID)
        .insert([rowToInsert]);

      console.log(`Successfully inserted call ${callData.call_id} into BigQuery.`);
    }

    res.status(200).send('Message processed successfully.');

  } catch (error) {
    console.error(`Error processing message:`, error);
    res.status(500).send('Error processing message');
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});