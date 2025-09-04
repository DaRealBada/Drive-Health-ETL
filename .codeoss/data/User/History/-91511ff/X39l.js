// src/sampling.js
// Deterministic hash-based sampling

const crypto = require('crypto');
const { logger } = require('./logger');

const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE || "1.0");

/**
 * Deterministic sampling based on idempotency key
 * @param {string} idempotencyKey - The unique key for the event
 * @param {number} auditRate - Sampling rate (0.0 to 1.0)
 * @returns {boolean} - True if event should be sampled
 */
function shouldSample(idempotencyKey, auditRate = AUDIT_RATE) {
  if (auditRate >= 1.0) return true;
  if (auditRate <= 0.0) return false;
  
  // Use SHA-256 for better security and distribution
  const hash = crypto.createHash('sha256').update(idempotencyKey).digest('hex');
  const hashValue = parseInt(hash.substring(0, 8), 16) / 0xffffffff;
  
  return hashValue < auditRate;
}

/**
 * Log sampling decision with structured metadata
 * @param {string} idempotencyKey - The unique key for the event
 * @param {Object} envelope - The event envelope
 * @param {boolean} sampled - Whether the event was sampled
 */
function logSamplingDecision(idempotencyKey, envelope, sampled) {
  const logMetadata = {
    idempotencyKey,
    tenant_id: envelope.tenant_id,
    event_type: envelope.event_type,
    trace_id: envelope.trace_id,
    sampled,
    audit_rate: AUDIT_RATE
  };
  
  if (sampled) {
    logger.info('Event sampled for processing', logMetadata);
  } else {
    logger.info('Event not sampled', logMetadata);
  }
}

/**
 * Evaluate sampling for an event and log the decision
 * @param {string} idempotencyKey - The unique key for the event
 * @param {Object} envelope - The event envelope
 * @param {number} auditRate - Optional override for audit rate
 * @returns {boolean} - True if event should be sampled
 */
function evaluateSampling(idempotencyKey, envelope, auditRate = AUDIT_RATE) {
  const sampled = shouldSample(idempotencyKey, auditRate);
  logSamplingDecision(idempotencyKey, envelope, sampled);
  return sampled;
}

module.exports = {
  shouldSample,
  logSamplingDecision,
  evaluateSampling,
  AUDIT_RATE
};