// src/bq.js
// BigQuery write operations with idempotency

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
    trace_id: envelope.trace_id,
    occurred_at: envelope.occurred_at,
    received_at: new Date().toISOString(),
    source: envelope.source || 'unknown',
    sampled: true,
    idempotency_key: idempotencyKey,
    payload: JSON.stringify(processedPayload),
    insertId: idempotencyKey, // CRITICAL: This ensures idempotency
  };
}

/**
 * Write a single event to BigQuery with idempotency
 * @param {Object} envelope - The validated event envelope
 * @param {Object} processedPayload - The normalized payload
 * @param {string} idempotencyKey - The idempotency key
 * @returns {Promise<Object>} - Success result with metadata
 */
async function writeEventToBigQuery(envelope, processedPayload, idempotencyKey) {
  const startTime = Date.now();
  
  try {
    const rowToInsert = createBigQueryRow(envelope, processedPayload, idempotencyKey);
    
    // Insert with idempotency protection
    await bigquery
      .dataset(BQ_DATASET)
      .table(BQ_TABLE)
      .insert([rowToInsert]);

    const processingTime = Date.now() - startTime;
    
    const logMetadata = {
      idempotencyKey,
      tenant_id: envelope.tenant_id,
      event_type: envelope.event_type,
      trace_id: envelope.trace_id,
      sampled: true,
      insert_status: 'SUCCESS',
      processing_time_ms: processingTime
    };
    
    logger.info('BigQuery insert success', logMetadata);
    
    return {
      success: true,
      processingTime,
      logMetadata
    };
    
  } catch (error) {
    const processingTime = Date.now() - startTime;
    
    const errorMetadata = {
      idempotencyKey,
      tenant_id: envelope.tenant_id,
      event_type: envelope.event_type,
      trace_id: envelope.trace_id,
      error: error.message,
      insert_status: 'ERROR',
      processing_time_ms: processingTime
    };
    
    logger.error('BigQuery insert failed', errorMetadata);
    
    throw error; // Re-throw to let handler decide on retry strategy
  }
}

/**
 * Write multiple events to BigQuery in batch (for future batching support)
 * @param {Array} events - Array of {envelope, processedPayload, idempotencyKey} objects
 * @returns {Promise<Object>} - Batch result with success/failure counts
 */
async function writeBatchToBigQuery(events) {
  const startTime = Date.now();
  
  try {
    const rowsToInsert = events.map(({ envelope, processedPayload, idempotencyKey }) => 
      createBigQueryRow(envelope, processedPayload, idempotencyKey)
    );
    
    await bigquery
      .dataset(BQ_DATASET)
      .table(BQ_TABLE)
      .insert(rowsToInsert);

    const processingTime = Date.now() - startTime;
    
    logger.info('BigQuery batch insert success', {
      batch_size: events.length,
      processing_time_ms: processingTime,
      insert_status: 'BATCH_SUCCESS'
    });
    
    return {
      success: true,
      count: events.length,
      processingTime
    };
    
  } catch (error) {
    const processingTime = Date.now() - startTime;
    
    logger.error('BigQuery batch insert failed', {
      batch_size: events.length,
      error: error.message,
      processing_time_ms: processingTime,
      insert_status: 'BATCH_ERROR'
    });
    
    throw error;
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
  writeEventToBigQuery,
  writeBatchToBigQuery,
  getTableInfo,
  BQ_DATASET,
  BQ_TABLE
};