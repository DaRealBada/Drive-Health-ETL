const express = require('express');
// NEW: Import the BigQuery client library
const { BigQuery } = require('@google-cloud/bigquery');
const { parsePhoneNumber } = require('libphonenumber-js');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 8080;

// NEW: Create the BigQuery client instance
const bigquery = new BigQuery();

// Configuration from environment variables
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE) || 1.0; // Default keep-all
const DEFAULT_PHONE_REGION = process.env.DEFAULT_PHONE_REGION || 'US';
// NEW: Add BigQuery dataset and table configuration
const BQ_DATASET = process.env.BQ_DATASET || 'drivehealth_dw';
const BQ_TABLE = process.env.BQ_TABLE || 'events';


app.use(express.json());

// Health check
app.get('/', (req, res) => {
  res.send('DriveHealth ETL Service is running!');
});

/**
 * Validates the envelope structure and required fields
 */
function validateEnvelope(envelope) {
  const required = [
    'envelope_version', 
    'event_type', 
    'schema_version',
    'tenant_id', 
    'occurred_at', 
    'payload'
  ];
  
  const missing = required.filter(field => !envelope[field]);
  
  if (missing.length > 0) {
    throw new Error(`Missing required fields: ${missing.join(', ')}`);
  }
  
  if (isNaN(Date.parse(envelope.occurred_at))) {
    throw new Error('occurred_at must be a valid ISO date string');
  }
  
  return true;
}

/**
 * Computes idempotency key from payload
 */
function computeIdempotencyKey(envelope) {
  const { payload, trace_id } = envelope;
  const key = payload.call_id || payload.message_id || trace_id;
  if (!key) {
    throw new Error('No idempotency key found: missing call_id, message_id, and trace_id');
  }
  return key;
}

/**
 * Deterministic sampling based on idempotency key
 */
function shouldSample(idempotencyKey, auditRate) {
  if (auditRate >= 1.0) return true;
  if (auditRate <= 0) return false;
  const hash = crypto.createHash('md5').update(idempotencyKey).digest('hex');
  const hashValue = parseInt(hash.substring(0, 8), 16) / 0xffffffff;
  return hashValue < auditRate;
}

/**
 * Normalize phone number to E.164 format
 */
function normalizePhone(phoneInput, region = DEFAULT_PHONE_REGION) {
  try {
    if (!phoneInput) return null;
    const phoneNumber = parsePhoneNumber(phoneInput, region);
    if (phoneNumber && phoneNumber.isValid()) {
      return phoneNumber.format('E.164');
    }
    return null;
  } catch (error) {
    console.warn(`Phone normalization failed for ${phoneInput}:`, error.message);
    return null;
  }
}

/**
 * Process and normalize payload data
 */
function processPayload(payload) {
  const processed = { ...payload };
  if (processed.caller) {
    processed.caller = normalizePhone(processed.caller);
  }
  if (processed.callee) {
    processed.callee = normalizePhone(processed.callee);
  }
  return processed;
}

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