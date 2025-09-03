// src/validation.js
// Envelope validation and idempotency key computation

const { logger } = require('./logger');

/**
 * Validates the envelope structure and required fields
 * @param {Object} envelope - The event envelope to validate
 * @returns {boolean} - True if valid
 * @throws {Error} - If validation fails
 */
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

/**
 * Computes idempotency key from payload
 * @param {Object} envelope - The event envelope
 * @returns {string} - The idempotency key
 * @throws {Error} - If no key can be found
 */
function computeIdempotencyKey(envelope) {
  const { payload, trace_id } = envelope;
  const key = payload.call_id || payload.message_id || trace_id;
  
  if (!key) {
    throw new Error('No idempotency key found');
  }
  
  return key;
}

/**
 * Validates and extracts idempotency key from envelope
 * @param {Object} envelope - The event envelope
 * @returns {Object} - { isValid: boolean, idempotencyKey: string, errors: string[] }
 */
function validateAndExtractKey(envelope) {
  const errors = [];
  let idempotencyKey = null;
  
  try {
    validateEnvelope(envelope);
    idempotencyKey = computeIdempotencyKey(envelope);
    return { isValid: true, idempotencyKey, errors: [] };
  } catch (error) {
    errors.push(error.message);
    logger.warn('Validation failed', { 
      error: error.message,
      tenant_id: envelope?.tenant_id,
      event_type: envelope?.event_type 
    });
    return { isValid: false, idempotencyKey, errors };
  }
}

module.exports = {
  validateEnvelope,
  computeIdempotencyKey,
  validateAndExtractKey
};