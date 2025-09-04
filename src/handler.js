// src/handler.js
// Main Pub/Sub endpoint handler with error categorization and micro-batching

const { logger } = require('./logger');
const { validateAndExtractKey } = require('./validation');
const { processPayload } = require('./phone');
const { evaluateSampling } = require('./sampling');
const { writeEventToBigQuery, writeBatchToBigQuery } = require('./bq');

// Micro-batching configuration
const MAX_BATCH_SIZE = parseInt(process.env.MAX_BATCH_SIZE) || 1;
const MAX_BATCH_WAIT_MS = parseInt(process.env.MAX_BATCH_WAIT_MS) || 100;
const ENABLE_BATCHING = MAX_BATCH_SIZE > 1;

// Batch state management
let batchQueue = [];
let batchTimer = null;
let pendingBatchResponses = [];

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
 * Flush the current batch to BigQuery
 * @returns {Promise<void>}
 */
async function flushBatch() {
  if (batchQueue.length === 0) return;
  
  const currentBatch = [...batchQueue];
  const currentResponses = [...pendingBatchResponses];
  
  // Clear batch state
  batchQueue.length = 0;
  pendingBatchResponses.length = 0;
  
  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }
  
  const batchStartTime = Date.now();
  
  logger.info('Processing batch', {
    batch_size: currentBatch.length,
    max_batch_size: MAX_BATCH_SIZE,
    wait_time_ms: MAX_BATCH_WAIT_MS
  });
  
  try {
    // Use existing writeBatchToBigQuery function
    const writeResult = await writeBatchToBigQuery(currentBatch);
    const batchProcessingTime = Date.now() - batchStartTime;
    
    // Resolve all pending responses with success
    currentResponses.forEach(({ resolve, originalProcessingTime, logMetadata }) => {
      resolve({
        success: true,
        sampled: true,
        statusCode: 204,
        processingTime: originalProcessingTime + batchProcessingTime,
        logMetadata: {
          ...logMetadata,
          batch_size: writeResult.count,
          batch_processing_time_ms: batchProcessingTime,
          total_processing_time_ms: originalProcessingTime + batchProcessingTime
        }
      });
    });
    
  } catch (error) {
    const batchProcessingTime = Date.now() - batchStartTime;
    const errorCategory = categorizeError(error);
    
    logger.error('Batch write failed', {
      batch_size: currentBatch.length,
      error: error.message,
      error_type: errorCategory.errorType,
      batch_processing_time_ms: batchProcessingTime,
      stack: error.stack
    });
    
    // Resolve all pending responses with batch error
    currentResponses.forEach(({ resolve, originalProcessingTime, envelope, idempotencyKey }) => {
      const errorMetadata = {
        idempotencyKey,
        tenant_id: envelope?.tenant_id,
        event_type: envelope?.event_type,
        trace_id: envelope?.trace_id,
        error: error.message,
        error_type: errorCategory.errorType,
        insert_status: errorCategory.isTerminal ? 'TERMINAL_ERROR' : 'TRANSIENT_ERROR',
        processing_time_ms: originalProcessingTime + batchProcessingTime,
        batch_size: currentBatch.length,
        batch_processing_time_ms: batchProcessingTime
      };
      
      if (errorCategory.isTerminal) {
        logger.warn('Terminal batch error - messages will go to DLQ', errorMetadata);
      } else {
        logger.error('Transient batch error - messages will be retried', {
          ...errorMetadata,
          stack: error.stack
        });
      }
      
      resolve({
        success: false,
        statusCode: errorCategory.statusCode,
        errorType: errorCategory.errorType,
        isTerminal: errorCategory.isTerminal,
        error: error.message,
        processingTime: originalProcessingTime + batchProcessingTime
      });
    });
  }
}

/**
 * Add processed message to batch queue
 * @param {Object} envelope - The validated event envelope
 * @param {Object} processedPayload - The normalized payload
 * @param {string} idempotencyKey - The idempotency key
 * @param {number} originalProcessingTime - Time spent in individual processing
 * @param {Object} logMetadata - Original log metadata
 * @returns {Promise<Object>} - Processing result
 */
async function addToBatch(envelope, processedPayload, idempotencyKey, originalProcessingTime, logMetadata) {
  return new Promise((resolve) => {
    // Add to batch queue
    batchQueue.push({ envelope, processedPayload, idempotencyKey });
    pendingBatchResponses.push({ 
      resolve, 
      originalProcessingTime, 
      logMetadata, 
      envelope, 
      idempotencyKey 
    });
    
    // Check if we should flush the batch
    if (batchQueue.length >= MAX_BATCH_SIZE) {
      // Flush immediately when batch is full
      setImmediate(() => flushBatch());
    } else if (batchQueue.length === 1 && MAX_BATCH_WAIT_MS > 0) {
      // Start timer for first message in batch
      batchTimer = setTimeout(() => flushBatch(), MAX_BATCH_WAIT_MS);
    }
  });
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
    
    const individualProcessingTime = Date.now() - startTime;
    
    // Choose processing path based on batching configuration
    if (ENABLE_BATCHING) {
      // Create log metadata for batch context
      const logMetadata = {
        idempotencyKey,
        tenant_id: envelope.tenant_id,
        event_type: envelope.event_type,
        trace_id: envelope.trace_id,
        sampled: true,
        insert_status: 'BATCHED',
        individual_processing_time_ms: individualProcessingTime
      };
      
      logger.info('Message queued for batch processing', logMetadata);
      
      // Add to batch and return promise that resolves when batch is processed
      return await addToBatch(envelope, processedPayload, idempotencyKey, individualProcessingTime, logMetadata);
      
    } else {
      // Original single-message processing path (unchanged)
      const writeResult = await writeBatchToBigQuery([{ envelope, processedPayload, idempotencyKey }]);
      
      return {
        success: true,
        sampled: true,
        statusCode: 204,
        processingTime: writeResult.processingTime,
        logMetadata: writeResult.logMetadata
      };
    }
    
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

/**
 * Expose batch flush function for external graceful shutdown handling
 * @returns {Promise<void>}
 */
async function flushPendingBatch() {
  if (batchQueue.length > 0) {
    logger.info('Flushing pending batch for shutdown', { 
      pending_count: batchQueue.length 
    });
    await flushBatch();
  }
}

/**
 * Get current batch state for monitoring/debugging
 * @returns {Object} - Current batch state
 */
function getBatchState() {
  return {
    currentBatchSize: batchQueue.length,
    hasPendingTimer: !!batchTimer,
    pendingResponses: pendingBatchResponses.length,
    batchConfig: {
      MAX_BATCH_SIZE,
      MAX_BATCH_WAIT_MS,
      ENABLE_BATCHING
    }
  };
}

module.exports = {
  processPubSubMessage,
  handlePubSubRequest,
  categorizeError,
  flushPendingBatch,
  getBatchState
};