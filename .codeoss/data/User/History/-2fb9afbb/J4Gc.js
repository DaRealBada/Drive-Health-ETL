const express = require('express');
const { BigQueryStorageClient } = require('@google-cloud/bigquery-storage');
// NEW: Import the built-in crypto library for hashing.
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 8080;

const writeClient = new BigQueryWriteClient();

// Configuration from environment variables
const DATASET_ID = process.env.DATASET_ID || 'call_audits';
const TABLE_ID = process.env.TABLE_ID || 'processed_calls';
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE) || 0.05;
const PROJECT_ID = process.env.GCP_PROJECT;

app.use(express.json());

// Health check endpoint
app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

// Single-message processing endpoint, triggered by Pub/Sub Push
app.post('/process-call', async (req, res) => {
  try {
    // NEW: Helper function to hash sensitive data.
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

    // 1. DECODE the single message from the request body
    const message = req.body.message;
    if (!message || !message.data) {
      console.warn('Invalid Pub/Sub message format.');
      return res.status(400).send('Bad Request');
    }
    const callData = JSON.parse(Buffer.from(message.data, 'base64').toString());

    // 2. VALIDATE the data
    if (!callData.call_id) {
      console.warn('Message missing call_id, acknowledging to avoid retries.');
      return res.status(200).send('Message acknowledged.'); 
    }

    // 3. SAMPLE the data
    if (Math.random() < AUDIT_RATE) {
      console.log(`Call ${callData.call_id} SELECTED for audit`);
      
      // UPDATED: Hash the data before transforming it.
      const hashedCallData = hashData(callData);

      // 4. TRANSFORM the data
      const rowToInsert = {
        call_id: callData.call_id,
        timestamp: callData.timestamp,
        duration: callData.duration,
        flagged_for_audit: true,
        processed_at: new Date().toISOString(),
        // UPDATED: Store the HASHED version of the data.
        original_metadata: JSON.stringify(hashedCallData)
      };

      // 5. LOAD the data using the Storage Write API
      const tablePath = `projects/${PROJECT_ID}/datasets/${DATASET_ID}/tables/${TABLE_ID}`;
      const writeStream = await writeClient.appendRows({ parent: tablePath });

      await writeStream.append([rowToInsert]); 
      await writeStream.finalize();

      console.log(`Successfully streamed call ${callData.call_id} into BigQuery.`);
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