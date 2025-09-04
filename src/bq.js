// src/bq.js
// BigQuery write operations with idempotency

const { BigQuery } = require('@google-cloud/bigquery');
const { logger } = require('./logger');

const bigquery = new BigQuery();

// Configuration from environment variables
const BQ_DATASET = process.env.BQ_DATASET || 'drivehealth_dw';
const BQ_TABLE = process.env.BQ_TABLE || 'events';
const PARTITION_TTL_DAYS = parseInt(process.env.PARTITION_TTL_DAYS) || 365;

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
    trace_id: envelope.trace_id || null, // FIX: Handle missing trace_id
    occurred_at: envelope.occurred_at,
    received_at: new Date().toISOString(),
    source: envelope.source || 'unknown',
    sampled: true,
    idempotency_key: idempotencyKey,
    payload: processedPayload, // FIX: Don't stringify JSON field
  };
}

/**
 * Write a single event to BigQuery with idempotency
 * @param {Object} envelope - The validated event envelope
 * @param {Object} processedPayload - The normalized payload
 * @param {string} idempotencyKey - The idempotency key
 * @returns {Promise<Object>} - Success result with metadata
 */
async function writeBatchToBigQuery(events) {
  const startTime = Date.now();

  try {
    // Create just the row data (no insertId in the data)
    const rowsToInsert = events.map(({ envelope, processedPayload, idempotencyKey }) =>
      createBigQueryRow(envelope, processedPayload, idempotencyKey)
    );

    console.log('=== BIGQUERY INSERT ATTEMPT ===');
    console.log('Rows to insert:', JSON.stringify(rowsToInsert, null, 2));
    console.log('===============================');

    // Use the raw property to pass insertId correctly
    const insertOptions = {
      raw: true, // Use raw mode for proper insertId handling
      rows: events.map(({ envelope, processedPayload, idempotencyKey }) => ({
        insertId: idempotencyKey, // This is the proper way to set insertId
        json: createBigQueryRow(envelope, processedPayload, idempotencyKey)
      }))
    };

    await bigquery
      .dataset(BQ_DATASET)
      .table(BQ_TABLE)
      .insert(insertOptions.rows, { raw: true });

    const processingTime = Date.now() - startTime;
    logger.info('BigQuery batch insert success', {
      batch_size: events.length,
      processing_time_ms: processingTime,
      insert_status: 'BATCH_SUCCESS'
    });

    return { success: true, count: events.length, processingTime };

  } catch (error) {
    const processingTime = Date.now() - startTime;

    console.log('=== BIGQUERY BATCH INSERT FAILED ===');
    console.log('Error message:', error.message);

    if (error.errors && Array.isArray(error.errors)) {
      console.log('Detailed row-level errors:');
      error.errors.forEach((err, index) => {
        console.log(`Error ${index + 1}:`, JSON.stringify(err, null, 2));
      });
    }
    console.log('====================================');

    logger.error('BigQuery batch insert failed', {
      batch_size: events.length,
      error: error.message,
      processing_time_ms: processingTime,
      insert_status: "BATCH_ERROR"
    });

    return { success: false, errors: error.errors || [error] };
  }
}

/**
 * Ensures the BigQuery table exists with the correct schema, partitioning, and clustering.
 */
async function ensureTable() {
  const table = bigquery.dataset(BQ_DATASET).table(BQ_TABLE);

  try {
    await table.get();
    console.log(`Table ${BQ_DATASET}.${BQ_TABLE} already exists.`);
  } catch (error) {
    if (error.code === 404) {
      console.log(`Table ${BQ_DATASET}.${BQ_TABLE} not found. Creating table...`);

      const schema = [
        { name: 'tenant_id', type: 'STRING', mode: 'NULLABLE' },
        { name: 'event_type', type: 'STRING', mode: 'NULLABLE' },
        { name: 'schema_version', type: 'INTEGER', mode: 'NULLABLE' },
        { name: 'envelope_version', type: 'INTEGER', mode: 'NULLABLE' },
        { name: 'trace_id', type: 'STRING', mode: 'NULLABLE' },
        { name: 'occurred_at', type: 'TIMESTAMP', mode: 'NULLABLE' },
        { name: 'received_at', type: 'TIMESTAMP', mode: 'NULLABLE' },
        { name: 'source', type: 'STRING', mode: 'NULLABLE' },
        { name: 'sampled', type: 'BOOLEAN', mode: 'NULLABLE' },
        { name: 'idempotency_key', type: 'STRING', mode: 'NULLABLE' },
        {
          name: 'payload',
          type: 'RECORD',
          mode: 'NULLABLE',
          fields: [
            { name: 'call_id', type: 'STRING', mode: 'NULLABLE' },
            { name: 'caller', type: 'STRING', mode: 'NULLABLE' },
            { name: 'callee', type: 'STRING', mode: 'NULLABLE' },
            { name: 'duration', type: 'INTEGER', mode: 'NULLABLE' },
            { name: 'status', type: 'STRING', mode: 'NULLABLE' },
            { name: 'message_id', type: 'STRING', mode: 'NULLABLE' },
            { name: 'from_phone', type: 'STRING', mode: 'NULLABLE' },
            { name: 'to_phone', type: 'STRING', mode: 'NULLABLE' },
            { name: 'channel', type: 'STRING', mode: 'NULLABLE' },
            { name: 'text_length', type: 'INTEGER', mode: 'NULLABLE' },
            { name: 'metadata', type: 'JSON', mode: 'NULLABLE' }
          ],
        },
      ];

      const options = {
        schema: schema,
        timePartitioning: {
          type: 'DAY',
          field: 'occurred_at',
          expirationMs: PARTITION_TTL_DAYS * 24 * 60 * 60 * 1000,
        },
        clustering: { fields: ['tenant_id', 'event_type'] },
      };

      await bigquery
        .dataset(BQ_DATASET)
        .createTable(BQ_TABLE, options);
      console.log(`Table ${BQ_DATASET}.${BQ_TABLE} created successfully.`);
    } else {
      throw error;
    }
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
  ensureTable,
  BQ_DATASET,
  BQ_TABLE
};