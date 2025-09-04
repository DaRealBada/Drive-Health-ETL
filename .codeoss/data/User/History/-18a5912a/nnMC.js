const express = require('express');
const { BigQueryStorageClient } = require('@google-cloud/bigquery-storage');
const writeClient = new BigQueryStorageClient();
const app = express();
const PORT = process.env.PORT || 8080;


// Configuration from environment variables
const DATASET_ID = process.env.DATASET_ID || 'call_audits';
const TABLE_ID = process.env.TABLE_ID || 'processed_calls';
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE) || 0.05;
const PROJECT_ID = process.env.GCP_PROJECT; // Automatically available in Cloud Run

app.use(express.json());

// Health check endpoint
app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

// Single-message processing endpoint, triggered by Pub/Sub Push
app.post('/process-call', async (req, res) => {
  try {
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
      // Acknowledge by sending a 200 OK, so the bad message is removed from the queue
      return res.status(200).send('Message acknowledged.'); 
    }

    // 3. SAMPLE the data
    if (Math.random() < AUDIT_RATE) {
      console.log(`Call ${callData.call_id} SELECTED for audit`);
      
      // 4. TRANSFORM the data
      const rowToInsert = {
        call_id: callData.call_id,
        timestamp: callData.timestamp,
        duration: callData.duration,
        flagged_for_audit: true,
        processed_at: new Date().toISOString(),
        original_metadata: JSON.stringify(callData)
      };

      // 5. LOAD the data using the Storage Write API
      const tablePath = `projects/${PROJECT_ID}/datasets/${DATASET_ID}/tables/${TABLE_ID}`;
      const writeStream = await writeClient.appendRows({ parent: tablePath });

      // Send a "batch" of one row
      await writeStream.append([rowToInsert]); 
      await writeStream.finalize();

      console.log(`Successfully streamed call ${callData.call_id} into BigQuery.`);
    }

    // Send a success response to Pub/Sub to acknowledge the message
    res.status(200).send('Message processed successfully.');

  } catch (error) {
    console.error(`Error processing message:`, error);
    // Send a 500 error to Pub/Sub to signal that the message should be retried
    res.status(500).send('Error processing message');
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});