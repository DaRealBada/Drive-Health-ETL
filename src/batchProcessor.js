// src/batchProcessor.js
// Handles micro-batching logic for BigQuery inserts.

const { logger } = require('./logger');
const { writeBatchToBigQuery } = require('./bq');

// highlight: Config defaults updated for immediate flush as the base plan.
const MAX_BATCH_SIZE = parseInt(process.env.MAX_BATCH_SIZE) || 1;
const MAX_BATCH_WAIT_MS = parseInt(process.env.MAX_BATCH_WAIT_MS) || 0; // fix: Default to 0 for immediate flush
const ENABLE_BATCHING = MAX_BATCH_SIZE > 1;

// Batch state management
let batchQueue = [];
let batchTimer = null;
let pendingBatchResponses = [];

/**
 * Maps a BigQuery row error reason to a proper HTTP status and error type.
 * @param {string} reason - The reason code from the BigQuery error object.
 * @returns {{statusCode: number, isTerminal: boolean, errorType: string}}
 */
function mapBqErrorReason(reason) {
  switch (reason) {
    case 'duplicate':
      // Idempotency key match is a success condition (idempotent POST).
      return { statusCode: 204, isTerminal: true, errorType: 'duplicate' };
    case 'invalid':
      // Schema or data validation error. Not retryable.
      return { statusCode: 422, isTerminal: true, errorType: 'validation_error' };
    case 'timeout':
    case 'stopped':
    default:
      // Transient or unknown errors that should be retried.
      return { statusCode: 503, isTerminal: false, errorType: 'transient_error' };
  }
}

/**
 * Flushes the current batch to BigQuery with per-row error handling.
 */
async function flushBatch() {
  if (batchQueue.length === 0) return;

  const currentBatch = [...batchQueue];
  const currentResponses = [...pendingBatchResponses];
  batchQueue.length = 0;
  pendingBatchResponses.length = 0;
  if (batchTimer) {
    clearTimeout(batchTimer);
    batchTimer = null;
  }

  const batchStartTime = Date.now();
  logger.info('Processing batch', { batch_size: currentBatch.length });

  try {
    const writeResult = await writeBatchToBigQuery(currentBatch);
    const batchProcessingTime = Date.now() - batchStartTime;

    // fix: Explicitly check writeResult.success to handle both returns and throws.
    if (writeResult.success) {
      // All rows succeeded.
      currentResponses.forEach(({ resolve }) => resolve({ success: true, statusCode: 204, processingTime: batchProcessingTime }));
    } else {
      // fix: Per-row error mapping for partial and total failures.
      const bqError = writeResult.errors[0];
      if (bqError.name === 'PartialFailureError' && bqError.errors?.length > 0) {
        // --- PARTIAL FAILURE ---
        logger.warn('Batch write partial failure', {
          success_count: currentBatch.length - bqError.errors.length,
          failure_count: bqError.errors.length
        });
        const failedRowDetails = new Map(bqError.errors.map(e => [e.index, e.errors[0]]));
        
        currentResponses.forEach(({ resolve }, index) => {
          if (failedRowDetails.has(index)) {
            const rowError = failedRowDetails.get(index);
            const errorCategory = mapBqErrorReason(rowError.reason);
            
            // For duplicates, we resolve as success (204).
            if (errorCategory.errorType === 'duplicate') {
              resolve({ success: true, statusCode: 204, processingTime: batchProcessingTime });
            } else {
              resolve({ success: false, ...errorCategory, error: rowError.message, processingTime: batchProcessingTime });
            }
          } else {
            // This row was successful.
            resolve({ success: true, statusCode: 204, processingTime: batchProcessingTime });
          }
        });
      } else {
        // --- TOTAL FAILURE ---
        logger.error('Batch write failed completely', { error: bqError.message });
        currentResponses.forEach(({ resolve }) => resolve({ success: false, statusCode: 503, isTerminal: false, error: bqError.message, processingTime: batchProcessingTime }));
      }
    }
  } catch (unexpectedError) {
    // Fallback for unexpected Javascript errors.
    logger.error('Unexpected error during flushBatch', { error: unexpectedError.message });
    currentResponses.forEach(({ resolve }) => resolve({ success: false, statusCode: 500, isTerminal: false, error: 'Unexpected batch processing error.' }));
  }
}

/**
 * Adds a processed message to the batch queue.
 * @returns {Promise<Object>} - A promise that resolves when the batch is flushed.
 */
function queueForBatch(envelope, processedPayload, idempotencyKey, originalProcessingTime, logMetadata) {
  return new Promise((resolve) => {
    batchQueue.push({ envelope, processedPayload, idempotencyKey });
    pendingBatchResponses.push({ resolve, originalProcessingTime, logMetadata, envelope, idempotencyKey });

    if (batchQueue.length >= MAX_BATCH_SIZE) {
      setImmediate(flushBatch);
    } else if (!batchTimer && batchQueue.length > 0) {
      batchTimer = setTimeout(flushBatch, MAX_BATCH_WAIT_MS);
    }
  });
}

/**
 * Flushes any pending messages before shutdown.
 */
async function flushPendingBatch() {
  if (batchQueue.length > 0) {
    logger.info('Flushing pending batch before shutdown', { pending_count: batchQueue.length });
    await flushBatch();
  }
}

function getBatchState() {
  return { currentBatchSize: batchQueue.length, config: { MAX_BATCH_SIZE, MAX_BATCH_WAIT_MS, ENABLE_BATCHING } };
}

module.exports = {
  queueForBatch,
  flushPendingBatch,
  getBatchState,
  ENABLE_BATCHING
};