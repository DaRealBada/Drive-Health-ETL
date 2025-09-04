// Import the required libraries
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const { parsePhoneNumber } = require('libphonenumber-js');
const crypto = require('crypto');
const { validateEnvelope } = require('./logger');

// Create the Express app; 
const app = express();
// Create the BigQuery client instance
const bigquery = new BigQuery();
// Configuration from environment variables or default to 8080
const PORT = process.env.PORT || 8080;
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE) || 1.0; // Default keep-all
const DEFAULT_PHONE_REGION = process.env.DEFAULT_PHONE_REGION || 'US';
const BQ_DATASET = process.env.BQ_DATASET || 'drivehealth_dw';
const BQ_TABLE = process.env.BQ_TABLE || 'events';

// Configures express app to accept json
app.use(express.json());

// General server health check
app.get('/', (req, res) => {
  res.send('DriveHealth ETL Service is running!');
});





// Main Pub/Sub push endpoint
app.post('/pubsub', async (req, res) => {
  let idempotencyKey;
  try {
    const message = req.body.message;
    if (!message || !message.data) {
      return res.status(400).json({ error: 'Invalid Pub/Sub message format' });
    }
    const envelope = JSON.parse(Buffer.from(message.data, 'base64').toString());
    
    validateEnvelope(envelope);
    idempotencyKey = computeIdempotencyKey(envelope);
    
    if (!shouldSample(idempotencyKey, AUDIT_RATE)) {
      return res.status(204).send(); // Acknowledge without storing [cite: 36]
    }
    
    const processedPayload = processPayload(envelope.payload);
    
    const rowToInsert = {
      tenant_id: envelope.tenant_id,
      event_type: envelope.event_type,
      schema_version: envelope.schema_version,
      envelope_version: envelope.envelope_version,
      trace_id: envelope.trace_id,
      occurred_at: envelope.occurred_at,
      received_at: new Date().toISOString(),
      source: envelope.source || 'unknown',
      sampled: true,
      idempotency_key: idempotencyKey,
      payload: JSON.stringify(processedPayload),
      // UPDATED: Add the insertId for BigQuery idempotency 
      insertId: idempotencyKey,
    };
    
    // UPDATED: This is the BigQuery write operation for Milestone B [cite: 33]
    await bigquery
      .dataset(BQ_DATASET)
      .table(BQ_TABLE)
      .insert([rowToInsert]);

    console.log('Successfully inserted record into BigQuery:', { idempotency_key: idempotencyKey });
    
    // Return success acknowledgement [cite: 36]
    res.status(204).send();
    
  } catch (error) {
    // UPDATED: Distinguish between terminal (4xx) and transient (5xx) errors
    if (error.message.includes('Missing required fields') || error.message.includes('No idempotency key')) {
      console.warn('Validation failed:', error.message);
      return res.status(400).json({ error: error.message }); // 4xx sends to DLQ [cite: 37]
    }
    
    console.error('Error processing message:', error);
    // 5xx signals a retry to Pub/Sub [cite: 38, 252]
    return res.status(503).json({ error: 'Internal processing error, please retry' });
  }
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Received SIGTERM, shutting down gracefully');
  process.exit(0);
});

const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

module.exports = { validateEnvelope, computeIdempotencyKey, shouldSample, normalizePhone, processPayload, server };