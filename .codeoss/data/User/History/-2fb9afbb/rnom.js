const express = require('express');
const { parsePhoneNumber } = require('libphonenumber-js');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 8080;

// Configuration from environment variables
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE) || 1.0; // Default keep-all
const DEFAULT_PHONE_REGION = process.env.DEFAULT_PHONE_REGION || 'US';

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
  
  // Validate occurred_at is a valid ISO date
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
  
  // Priority order: call_id, message_id, trace_id
  const key = payload.call_id || payload.message_id || trace_id;
  
  if (!key) {
    throw new Error('No idempotency key found: missing call_id, message_id, and trace_id');
  }
  
  return key;
}

/**
 * Deterministic sampling based on idempotency key. The same message will always have the same sampling decision.
 */
function shouldSample(idempotencyKey, auditRate) {
  if (auditRate >= 1.0) return true;
  if (auditRate <= 0) return false;
  
  // Create deterministic hash of the key. Use md5 standard algorithm, feed idempotency key into algorithm, output hexadecimal.
  const hash = crypto.createHash('md5').update(idempotencyKey).digest('hex');
  // Convert first 8 chars to integer and normalize to 0-1 by dividing string by largest possible 8-char hexadecimal.
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
    
    return null; // Invalid phone number
  } catch (error) {
    console.warn(`Phone normalization failed for ${phoneInput}:`, error.message);
    return null;
  }
}

/**
 * Process and normalize payload data
 */
function processPayload(payload, eventType) {
  const processed = { ...payload };
  
  // Normalize phone numbers if present
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
  try {
    console.log('Received Pub/Sub push message');
    
    // Extract envelope from Pub/Sub message
    const message = req.body.message;
    if (!message || !message.data) {
      return res.status(400).json({ error: 'Invalid Pub/Sub message format' });
    }
    
    // Decode base64 message data
    const envelope = JSON.parse(Buffer.from(message.data, 'base64').toString());
    
    console.log('Processing envelope:', {
      event_type: envelope.event_type,
      tenant_id: envelope.tenant_id,
      trace_id: envelope.trace_id
    });
    
    // Step 1: Validate envelope
    try {
      validateEnvelope(envelope);
    } catch (validationError) {
      console.warn('Validation failed:', validationError.message);
      return res.status(400).json({ error: validationError.message });
    }
    
    // Step 2: Compute idempotency key
    let idempotencyKey;
    try {
      idempotencyKey = computeIdempotencyKey(envelope);
    } catch (keyError) {
      console.warn('Idempotency key computation failed:', keyError.message);
      return res.status(400).json({ error: keyError.message });
    }
    
    // Step 3: Determine if this event should be sampled
    const sampled = shouldSample(idempotencyKey, AUDIT_RATE);
    
    console.log(`Event ${idempotencyKey} sampling decision:`, sampled ? 'SELECTED' : 'SKIPPED');
    
    if (!sampled) {
      // Event not selected for processing - acknowledge success
      return res.status(204).send(); // No content - processed but not stored
    }
    
    // Step 4: Process payload (normalize phones, etc.)
    const processedPayload = processPayload(envelope.payload, envelope.event_type);
    
    // Step 5: Create normalized row object (ready for BigQuery)
    const normalizedRow = {
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
      payload: processedPayload
    };
    
    // For Milestone A: Just log the normalized row (BigQuery integration comes in Milestone B)
    console.log('Normalized row ready for storage:', {
      idempotency_key: normalizedRow.idempotency_key,
      event_type: normalizedRow.event_type,
      tenant_id: normalizedRow.tenant_id,
      sampled: normalizedRow.sampled,
      payload_caller: normalizedRow.payload.caller, // Show E.164 normalization worked
      payload_callee: normalizedRow.payload.callee
    });
    
    // Return success
    res.status(200).json({ 
      message: 'Event processed successfully',
      idempotency_key: idempotencyKey,
      sampled: true
    });
    
  } catch (error) {
    console.error('Error processing message:', error);
    
    // Return 500 for transient errors (will trigger Pub/Sub retry)
    res.status(500).json({ error: 'Internal processing error' });
  }
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Received SIGTERM, shutting down gracefully');
  process.exit(0);
});

const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Configuration: AUDIT_RATE=${AUDIT_RATE}, DEFAULT_PHONE_REGION=${DEFAULT_PHONE_REGION}`);
});
module.exports = { validateEnvelope, computeIdempotencyKey, shouldSample, normalizePhone, processPayload, server };