const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const crypto = require('crypto');
const winston = require('winston');
const { parsePhoneNumber } = require('libphonenumber-js');

// --- SETUP & CONFIGURATION ---

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [ new winston.transports.Console() ],
});

const app = express();
const PORT = process.env.PORT || 8080;

// NEW: Create the BigQuery client instance
const bigquery = new BigQuery();

// Configuration from environment variables
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE || "1.0");
const DEFAULT_PHONE_REGION = process.env.DEFAULT_PHONE_REGION || 'US';
const BQ_DATASET = process.env.BQ_DATASET || 'drivehealth_dw';
const BQ_TABLE = process.env.BQ_TABLE || 'events';

app.use(express.json());

// --- HELPER FUNCTIONS ---

function validateEnvelope(envelope) {
  const required = ['envelope_version', 'event_type', 'schema_version', 'tenant_id', 'occurred_at', 'payload'];
  const missing = required.filter(field => !envelope[field]);
  if (missing.length > 0) {
    throw new Error(`Missing required fields: ${missing.join(', ')}`);
  }
  if (isNaN(Date.parse(envelope.occurred_at))) {
    throw new Error('occurred_at must be a valid ISO date string');
  }
  return true;
}

function computeIdempotencyKey(envelope) {
  const { payload, trace_id } = envelope;
  const key = payload.call_id || payload.message_id || trace_id;
  if (!key) {
    throw new Error('No idempotency key found');
  }
  return key;
}

function shouldSample(idempotencyKey, auditRate) {
  if (auditRate >= 1.0) return true;
  if (auditRate <= 0.0) return false;
  const hash = crypto.createHash('sha256').update(idempotencyKey).digest('hex');
  const hashValue = parseInt(hash.substring(0, 8), 16) / 0xffffffff;
  return hashValue < auditRate;
}

function normalizePhone(phoneInput, region = DEFAULT_PHONE_REGION) {
  try {
    if (!phoneInput) return null;
    const phoneNumber = parsePhoneNumber(phoneInput, region);
    return phoneNumber?.isValid() ? phoneNumber.format('E.164') : null;
  } catch (error) {
    logger.warn('Phone normalization failed', { phoneInput, error: error.message });
    return null;
  }
}

function processPayload(payload) {
    const processed = { ...payload };
    if (processed.caller) processed.caller = normalizePhone(processed.caller);
    if (processed.callee) processed.callee = normalizePhone(processed.callee);
    return processed;
}

// --- MAIN ENDPOINT ---

app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

app.post('/pubsub', async (req, res) => {
  let idempotencyKey;
  try {
    const message = req.body.message;
    if (!message?.data) {
      logger.warn('Invalid Pub/Sub message format');
      return res.status(400).send('Invalid message format');
    }
    const envelope = JSON.parse(Buffer.from(message.data, 'base64').toString());

    validateEnvelope(envelope);
    idempotencyKey = computeIdempotencyKey(envelope);

    if (!shouldSample(idempotencyKey, AUDIT_RATE)) {
      logger.info('Event not selected for sampling', { idempotencyKey });
      return res.status(204).send();
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
      payload: JSON.stringify(processedPayload), // Store the processed payload as a JSON string
    };

    // NEW: This is the BigQuery write operation
    await bigquery
      .dataset(BQ_DATASET)
      .table(BQ_TABLE)
      .insert([rowToInsert], {
        // This option is critical for idempotency
        rowId: idempotencyKey 
      });

    logger.info('Successfully inserted record into BigQuery', { idempotencyKey });

    res.status(204).send();

  } catch (error) {
    if (error.message.startsWith('Missing required fields') || error.message.startsWith('No idempotency key')) {
      logger.warn('Terminal validation error.', { error: error.message });
      return res.status(400).send(`Bad Request: ${error.message}`);
    }
    
    logger.error('Error processing message', { idempotencyKey, error: error.message, stack: error.stack });
    res.status(503).send('Internal Server Error');
  }
});

// --- SERVER START ---

app.listen(PORT, () => {
  logger.info(`ETL Service started`, { port: PORT });
});