const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const crypto = require('crypto');
// NEW: Import the Winston logging library
const winston = require('winston');

// NEW: Create a configured logger instance
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.Console(),
  ],
});

const app = express();
const PORT = process.env.PORT || 8080;

const bigquery = new BigQuery();

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
    const hashData = (data) => {
      const piiFields = ['caller', 'receiver'];
      const hashedData = { ...data };
      for (const field of piiFields) {
        if (hashedData[field]) {
          const hash = crypto.createHash('sha256').update(hashedData[field]);
          hashedData[field] = hash.digest('hex');
        }
      }
      return hashedData;
    };

    const message = req.body.message;
    if (!message || !message.data) {
      logger.warn('Invalid Pub/Sub message format received.');
      return res.status(400).send('Bad Request');
    }
    const callData = JSON.parse(Buffer.from(message.data, 'base64').toString());

    if (!callData.call_id) {
      logger.warn('Message missing call_id, acknowledging to avoid retries.');
      return res.status(200).send('Message acknowledged.'); 
    }

    if (Math.random() < AUDIT_RATE) {
      logger.info('Call selected for audit.', { callId: callData.call_id });
      
      const hashedCallData = hashData(callData);

      const rowToInsert = {
        call_id: callData.call_id,
        timestamp: callData.timestamp,
        duration: callData.duration,
        flagged_for_audit: true,
        processed_at: new Date().toISOString(),
        original_metadata: JSON.stringify(hashedCallData)
      };

      await bigquery
        .dataset(DATASET_ID)
        .table(TABLE_ID)
        .insert([rowToInsert]);

      logger.info('Successfully inserted call into BigQuery.', { callId: callData.call_id });
    }

    res.status(200).send('Message processed successfully.');

  } catch (error) {
    // UPDATED: Use the logger for detailed error information
    logger.error('Error processing message.', { 
      errorMessage: error.message, 
      errorStack: error.stack 
    });
    res.status(500).send('Error processing message');
  }
});

app.listen(PORT, () => {
  // UPDATED: Use the logger for server startup
  logger.info(`Server running on port ${PORT}`);
});