// src/handler.js
// Main Pub/Sub endpoint handler with error categorization.

const { logger } = require('./logger');
const { validateAndExtractKey } = require('./validation');
const { processPayload } = require('./phone');
const { evaluateSampling } = require('./sampling');
const { writeBatchToBigQuery } = require('./bq');
const { queueForBatch, flushPendingBatch, getBatchState } = require('./batchProcessor');

const MAX_BATCH_SIZE = parseInt(process.env.MAX_BATCH_SIZE) || 1;

/**
 * Determines if a pre-processing error is terminal (4xx) or transient (5xx).
 */
function categorizeError(error) {
  // fix: Add a check for the specific PartialFailureError from BigQuery.
  if (error.name === 'PartialFailureError') {
    return { isTerminal: false, statusCode: 503, errorType: 'transient_error' };
  }

  // Ensure error.message exists before trying to read it.
  const message = error.message ? error.message.toLowerCase() : '';

  if (error instanceof SyntaxError || message.includes('invalid json') || message.includes('malformed envelope')) {
    return { isTerminal: true, statusCode: 422, errorType: 'format_error' };
  }
  if (message.includes('missing required fields') || message.includes('no idempotency key') || message.includes('occurred_at must be a valid')) {
    return { isTerminal: true, statusCode: 400, errorType: 'validation_error' };
  }
  return { isTerminal: false, statusCode: 503, errorType: 'transient_error' };
}

/**
 * Processes a single Pub/Sub message.
 */
async function processPubSubMessage(message) {
  let envelope;
  let idempotencyKey;
  const startTime = Date.now();

  try {
    if (!message?.data) throw new Error('Invalid Pub/Sub message format');
    envelope = JSON.parse(Buffer.from(message.data, 'base64').toString());

    const validation = validateAndExtractKey(envelope);
    if (!validation.isValid) throw new Error(validation.errors.join('; '));
    idempotencyKey = validation.idempotencyKey;

    const shouldProcess = evaluateSampling(idempotencyKey, envelope);
    if (!shouldProcess) {
      logger.info('Event sampled out', {
        idempotencyKey: idempotencyKey,
        tenant_id: envelope.tenant_id,
        sampled: false
      });
      return { success: true, sampled: false, statusCode: 204, processingTime: Date.now() - startTime };
    }

    const processedPayload = processPayload(envelope.payload);
    const individualProcessingTime = Date.now() - startTime;

    if (MAX_BATCH_SIZE > 1) {
      const logMetadata = {
        idempotencyKey: idempotencyKey,
        tenant_id: envelope.tenant_id,
        event_type: envelope.event_type,
        trace_id: envelope.trace_id,
        sampled: true,
        insert_status: 'BATCHED',
        individual_processing_time_ms: individualProcessingTime
      };
      logger.info('Message queued for batch processing', logMetadata);
      return await queueForBatch(envelope, processedPayload, idempotencyKey, individualProcessingTime, logMetadata);
    } else {
      const writeResult = await writeBatchToBigQuery([{ envelope, processedPayload, idempotencyKey }]);
      if (!writeResult.success) throw writeResult.errors[0];
      
      return { 
        success: true, 
        statusCode: 204, 
        processingTime: writeResult.processingTime,
        logMetadata: writeResult.logMetadata
      };
    }
  } catch (error) {
    const processingTime = Date.now() - startTime;
    const errorCategory = categorizeError(error);
    const errorMessage = error.message || 'Batch insert failed with partial failures.';
    
    const errorMetadata = {
      idempotencyKey: idempotencyKey,
      tenant_id: envelope?.tenant_id,
      event_type: envelope?.event_type,
      trace_id: envelope?.trace_id,
      error: errorMessage,
      error_type: errorCategory.errorType,
      insert_status: errorCategory.isTerminal ? 'TERMINAL_ERROR' : 'TRANSIENT_ERROR',
      processing_time_ms: processingTime
    };

    if (errorCategory.isTerminal) {
      logger.warn('Terminal error - message will go to DLQ', errorMetadata);
    } else {
      logger.error('Transient error - message will be retried', errorMetadata);
    }
    return { success: false, ...errorCategory, error: errorMessage, processingTime };
  }
}

/**
 * Express route handler for /pubsub endpoint.
 */
async function handlePubSubRequest(req, res) {
  try {
    const result = await processPubSubMessage(req.body.message);
    if (result.success) {
      return res.status(result.statusCode).send();
    } else {
      const body = result.isTerminal ? `Bad Request: ${result.error}` : 'Internal Server Error';
      return res.status(result.statusCode).send(body);
    }
  } catch (error) {
    logger.error('Unexpected error in handler', {
      error: error.message,
    });
    return res.status(503).send('Internal Server Error');
  }
}

module.exports = {
  processPubSubMessage,
  handlePubSubRequest,
  categorizeError,
  flushPendingBatch,
  getBatchState,
};