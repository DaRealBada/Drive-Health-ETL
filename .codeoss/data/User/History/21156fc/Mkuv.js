// src/handler.js
// Main Pub/Sub endpoint handler with error categorization

const { logger } = require('./logger');
const { validateAndExtractKey } = require('./validation');
const { processPayload } = require('./phone');
const { evaluateSampling } = require('./sampling');
const { writeEventToBigQuery } = require('../src/bq');

/**
 * Determines if an error is terminal (4xx) or transient (5xx)
 * @param {Error} error - The error to categorize
 * @returns {Object} - { isTerminal: boolean, statusCode: number, errorType: string }
 */
function categorizeError(error) {
  const message = error.message.toLowerCase();
  
  // Terminal errors (client should not retry)
  if (message.includes('missing required fields') || 
      message.includes('no idempotency key') ||
      message.includes('occurred_at must be a valid')) {
    return {
      isTerminal: true,
      statusCode: 400,
      errorType: 'validation_error'
    };
  }
  
  // Add more terminal error patterns here as needed
  if (message.includes('invalid json') || 
      message.includes('malformed envelope')) {
    return {
      isTerminal: true,
      statusCode: 422,
      errorType: 'format_error'
    };
  }
  
  // Default to transient (service should retry)
  return {
    isTerminal: false,
    statusCode: 503,
    errorType: 'transient_error'
  };
}

/**
 * Process a single Pub/Sub message
 * @param {Object} message - Pub/Sub message object
 * @returns {Promise<Object>} - Processing result
 */
async function processPubSubMessage(message) {
  let envelope;
  let idempotencyKey;
  const startTime = Date.now();
  
  try {
    // Parse message data
    if (!message?.data) {
      throw new Error('Invalid Pub/Sub message format');
    }
    
    envelope = JSON.parse(Buffer.from(message.data, 'base64').toString());
    
    // Validate envelope and extract idempotency key
    const validation = validateAndExtractKey(envelope);
    if (!validation.isValid) {
      throw new Error(validation.errors.join('; '));
    }
    
    idempotencyKey = validation.idempotencyKey;
    
    // Check sampling
    const shouldProcess = evaluateSampling(idempotencyKey, envelope);
    if (!shouldProcess) {
      return {
        success: true,
        sampled: false,
        statusCode: 204,
        processingTime: Date.now() - startTime
      };
    }
    
    // Process payload (phone normalization, etc.)
    const processedPayload = processPayload(envelope.payload);
    
    // Write to BigQuery
    const writeResult = await writeEventToBigQuery(envelope, processedPayload, idempotencyKey);
    
    return {
      success: true,
      sampled: true,
      statusCode: 204,
      processingTime: writeResult.processingTime,
      logMetadata: writeResult.logMetadata
    };
    
  } catch (error) {
    const processingTime = Date.now() - startTime;
    const errorCategory = categorizeError(error);
    
    const errorMetadata = {
      idempotencyKey,
      tenant_id: envelope?.tenant_id,
      event_type: envelope?.event_type,
      trace_id: envelope?.trace_id,
      error: error.message,
      error_type: errorCategory.errorType,
      insert_status: errorCategory.isTerminal ? 'TERMINAL_ERROR' : 'TRANSIENT_ERROR',
      processing_time_ms: processingTime
    };
    
    if (errorCategory.isTerminal) {
      logger.warn('Terminal error - message will go to DLQ', errorMetadata);
    } else {
      logger.error('Transient error - message will be retried', {
        ...errorMetadata,
        stack: error.stack
      });
    }
    
    return {
      success: false,
      statusCode: errorCategory.statusCode,
      errorType: errorCategory.errorType,
      isTerminal: errorCategory.isTerminal,
      error: error.message,
      processingTime
    };
  }
}

/**
 * Express route handler for /pubsub endpoint
 * @param {Object} req - Express request
 * @param {Object} res - Express response
 */
async function handlePubSubRequest(req, res) {
  try {
    const result = await processPubSubMessage(req.body.message);
    
    if (result.success) {
      return res.status(result.statusCode).send();
    } else {
      // Return appropriate error status
      if (result.isTerminal) {
        return res.status(result.statusCode).send(`Bad Request: ${result.error}`);
      } else {
        return res.status(result.statusCode).send('Internal Server Error');
      }
    }
    
  } catch (error) {
    // Fallback error handling
    logger.error('Unexpected error in handler', { 
      error: error.message, 
      stack: error.stack 
    });
    return res.status(503).send('Internal Server Error');
  }
}

module.exports = {
  processPubSubMessage,
  handlePubSubRequest,
  categorizeError
};