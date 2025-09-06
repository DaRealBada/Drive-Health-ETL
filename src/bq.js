// src/bq.js
// BigQuery write operations with idempotency and JSON payload

const { BigQuery } = require('@google-cloud/bigquery');
const { logger } = require('./logger');

const bigquery = new BigQuery();

// Configuration from environment variables
const BQ_DATASET = process.env.BQ_DATASET || 'drivehealth_dw';
const BQ_TABLE = process.env.BQ_TABLE || 'events';

/**
 * Create a BigQuery row object from envelope and processed payload
 * @param {Object} envelope - The validated event envelope
 * @param {Object} processedPayload - The normalized payload
 * @param {string} idempotencyKey - The idempotency key
 * @returns {Object} - BigQuery row object
 */
function createBigQueryRow(envelope, processedPayload, idempotencyKey) {
  return {
    tenant_id: envelope.tenant_id,
    event_type: envelope.event_type,
    schema_version: envelope.schema_version,
    envelope_version: envelope.envelope_version,
    trace_id: envelope.trace_id || null,
    occurred_at: envelope.occurred_at,
    received_at: new Date().toISOString(),
    source: envelope.source || 'unknown',
    sampled: true,
    idempotencyKey: idempotencyKey,
    payload: processedPayload,
  };
}

/**
 * Write events to BigQuery in batch with idempotency
 * @param {Array} events - Array of {envelope, processedPayload, idempotencyKey} objects
 * @returns {Promise<Object>} - Success result with metadata
 */
async function writeBatchToBigQuery(events) {
  const startTime = Date.now();

  try {
    const insertOptions = {
      raw: true,
      rows: events.map(({ envelope, processedPayload, idempotencyKey }) => ({
        insertId: idempotencyKey,
        json: createBigQueryRow(envelope, processedPayload, idempotencyKey)
      }))
    };

    await bigquery
      .dataset(BQ_DATASET)
      .table(BQ_TABLE)
      .insert(insertOptions.rows, { raw: true });

    const processingTime = Date.now() - startTime;
    const logMetadata = {
      batch_size: events.length,
      processing_time_ms: processingTime,
      insert_status: 'BATCH_SUCCESS'
    };
    
    logger.info('BigQuery batch insert success', logMetadata);

    // fix: The 'logMetadata' object is now correctly included in the return value.
    return { 
      success: true, 
      count: events.length, 
      processingTime,
      logMetadata: logMetadata 
    };

  } catch (error) {
    // ... (the catch block remains unchanged)
    const processingTime = Date.now() - startTime;

    if (error.errors && error.errors.length > 0) {
      const failedRowCount = error.errors.length;
      const successRowCount = events.length - failedRowCount;

      logger.error('BigQuery batch insert partial failure', {
        batch_size: events.length,
        success_count: successRowCount,
        failure_count: failedRowCount,
        processing_time_ms: processingTime,
        insert_status: "BATCH_PARTIAL_FAILURE",
        sample_errors: error.errors.slice(0, 3).map(err => ({
            index: err.index,
            reason: err.errors[0]?.reason,
            idempotencyKey: events[err.index]?.idempotencyKey
        }))
      });
    } else {
      logger.error('BigQuery batch insert failed', {
        batch_size: events.length,
        error: error.message,
        error_code: error.code,
        processing_time_ms: processingTime,
        insert_status: "BATCH_ERROR",
        sample_keys: events.slice(0, 3).map(e => e.idempotencyKey)
      });
    }
    return { success: false, errors: error.errors || [error] };
  }
}


/**
 * Get BigQuery table information for monitoring
 * @returns {Promise<Object>} - Table metadata
 */
async function getTableInfo() {
  try {
    const [metadata] = await bigquery
      .dataset(BQ_DATASET)
      .table(BQ_TABLE)
      .getMetadata();

    return {
      tableId: metadata.tableReference.tableId,
      location: metadata.location,
      numRows: metadata.numRows,
      numBytes: metadata.numBytes,
      lastModified: metadata.lastModifiedTime
    };
  } catch (error) {
    logger.error('Failed to get table info', { error: error.message });
    throw error;
  }
}

module.exports = {
  createBigQueryRow,
  writeBatchToBigQuery,
  getTableInfo,
  BQ_DATASET,
  BQ_TABLE
};