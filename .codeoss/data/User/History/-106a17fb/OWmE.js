// src/phone.js
// E.164 phone number normalization

const { parsePhoneNumber } = require('libphonenumber-js');
const { logger } = require('./logger');

const DEFAULT_PHONE_REGION = process.env.DEFAULT_PHONE_REGION || 'US';

/**
 * Normalize phone number to E.164 format
 * @param {string} phoneInput - Raw phone number input
 * @param {string} region - Default region for parsing (e.g., 'US')
 * @returns {string|null} - E.164 formatted phone number or null if invalid
 */
function normalizePhone(phoneInput, region = DEFAULT_PHONE_REGION) {
  try {
    if (!phoneInput) return null;
    
    const phoneNumber = parsePhoneNumber(phoneInput, region);
    return phoneNumber?.isValid() ? phoneNumber.format('E.164') : null;
  } catch (error) {
    logger.warn('Phone normalization failed', { 
      phoneInput, 
      error: error.message,
      region 
    });
    return null;
  }
}

/**
 * Process and normalize phone numbers in payload data
 * @param {Object} payload - Event payload that may contain phone numbers
 * @returns {Object} - Processed payload with normalized phone numbers
 */
function processPayload(payload) {
  const processed = { ...payload };
  
  // Normalize common phone number fields
  if (processed.caller) {
    processed.caller = normalizePhone(processed.caller);
  }
  if (processed.callee) {
    processed.callee = normalizePhone(processed.callee);
  }
  
  // Add more phone number fields as needed based on your event types
  if (processed.from_phone) {
    processed.from_phone = normalizePhone(processed.from_phone);
  }
  if (processed.to_phone) {
    processed.to_phone = normalizePhone(processed.to_phone);
  }
  
  return processed;
}

module.exports = {
  normalizePhone,
  processPayload
};