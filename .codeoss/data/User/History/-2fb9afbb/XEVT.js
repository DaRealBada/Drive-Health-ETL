const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const { parsePhoneNumber } = require('libphonenumber-js');
const crypto = require('crypto');
// NEW for Milestone C: Structured logging with pino
const pino = require('pino');
// NEW for Milestone C: Cloud Monitoring client
const { Monitoring } = require('@google-cloud/monitoring');

const app = express();
const PORT = process.env.PORT || 8080;

// NEW for Milestone C: Initialize pino logger with structured format
const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level: (label) => {
      return { level: label };
    },
  },
  timestamp: pino.stdTimeFunctions.isoTime,
});

// Create the BigQuery client instance
const bigquery = new BigQuery();
// NEW for Milestone C: Initialize Cloud Monitoring client
const monitoring = new Monitoring.MetricServiceClient();

// Configuration from environment variables
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE) || 1.0;
const DEFAULT_PHONE_REGION = process.env.DEFAULT_PHONE_REGION || 'US';
const BQ_DATASET = process.env.BQ_DATASET || 'drivehealth_dw';
const BQ_TABLE = process.env.BQ_TABLE || 'events';
// NEW for Milestone C: Monitoring configuration
const PROJECT_ID = process.env.GOOGLE_CLOUD_PROJECT || process.env.GCP_PROJECT;
const SERVICE_NAME = process.env.SERVICE_NAME || 'drivehealth-etl';

app.use(express.json());

// NEW for Milestone C: Request logging middleware
app.use((req, res, next) => {
  const start = Date.now();
  const originalSend = res.send;
  
  res.send = function(data) {
    const duration = Date.now() - start;
    const logData = {
      method: req.method,
      url: req.url,
      status_code: res.statusCode,
      duration_ms: duration,
      user_agent: req.get('User-Agent'),
      content_length: res.get('Content-Length') || 0,
    };
    
    if (res.statusCode >= 400) {
      logger.warn('HTTP request completed with error', logData);
    } else {
      logger.info('HTTP request completed', logData);
    }
    
    originalSend.call(this, data);
  };
  
  next();
});

// Health check
app.get('/', (req, res) => {
  logger.info('Health check requested');
  res.send('DriveHealth ETL Service is running!');
});

// NEW for Milestone C: Health check endpoint for monitoring
app.get('/health', (req, res) => {
  // Perform basic health checks
  const health = {
    status: 'healthy',
    timestamp: new Date().toISOString(),
    service: SERVICE_NAME,
    version: process.env.SERVICE_VERSION || '1.0.0',
    uptime_seconds: Math.floor(process.uptime()),
  };
  
  logger.info('Health check performed', { health_status: health.status });
  res.status(200).json(health);
});

// NEW for Milestone C: Metrics endpoint for custom metrics
app.get('/metrics', (req, res) => {
  // Basic metrics that can be scraped
  const metrics = {
    audit_rate: AUDIT_RATE,
    uptime_seconds: Math.floor(process.uptime()),
    memory_usage: process.memoryUsage(),
    timestamp: new Date().toISOString(),
  };
  
  logger.info('Metrics requested', { metrics_requested: true });
  res.status(200).json(metrics);
});

/**
 * Validates the envelope structure and required fields
 */
function validateEnvelope(envelope) {
  const required = [
    'envelope_version', 
    'event_type', 
    'schema_version',
    'tenant_id', 
    'occurred_at', 
    'payload'
  ];
  
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
 */
function computeIdempotencyKey(envelope) {
  const { payload, trace_id } = envelope;
  const key = payload.call_id || payload.message_id || trace_id;
  if (!key) {
    throw new Error('No idempotency key found: missing call_id, message_id, and trace_id');
  }
  return key;
}

/**
 * Deterministic sampling based on idempotency key
 */
function shouldSample(idempotencyKey, auditRate) {
  if (auditRate >= 1.0) return true;
  if (auditRate <= 0) return false;
  const hash = crypto.createHash('md5').update(idempotencyKey).digest('hex');
  const hashValue = parseInt(hash.substring(0, 8), 16) / 0xffffffff;
  return hashValue < auditRate;
}

/**
 * Normalize phone number to E.164 format
 */
function normalizePhone(phoneInput, region = DEFAULT_PHONE_REGION) {
  try {
    if (!phoneInput) return null;
    const phoneNumber = parsePhoneNumber(phoneInput, region);
    if (phoneNumber && phoneNumber.isValid()) {
      return phoneNumber.format('E.164');
    }
    return null;
  } catch (error) {
    // NEW for Milestone C: Structured logging for phone normalization errors
    logger.warn('Phone normalization failed', {
      phone_input: phoneInput,
      error_message: error.message,
      error_type: 'phone_normalization_error'
    });
    return null;
  }
}

/**
 * Process and normalize payload data
 */
function processPayload(payload) {
  const processed = { ...payload };
  if (processed.caller) {
    processed.caller = normalizePhone(processed.caller);
  }
  if (processed.callee) {
    processed.callee = normalizePhone(processed.callee);
  }
  return processed;
}

// NEW for Milestone C: Write custom metric to Cloud Monitoring
async function writeCustomMetric(metricType, value, labels = {}) {
  if (!PROJECT_ID) {
    logger.warn('PROJECT_ID not set, skipping custom metric', { metric_type: metricType });
    return;
  }
  
  try {
    const request = {
      name: monitoring.projectPath(PROJECT_ID),
      timeSeries: [
        {
          metric: {
            type: `custom.googleapis.com/${SERVICE_NAME}/${metricType}`,
            labels: labels,
          },
          resource: {
            type: 'cloud_run_revision',
            labels: {
              project_id: PROJECT_ID,
              service_name: SERVICE_NAME,
              revision_name: process.env.K_REVISION || 'local',
              location: process.env.GOOGLE_CLOUD_REGION || 'us-central1',
            },
          },
          points: [
            {
              interval: {
                endTime: {
                  seconds: Math.floor(Date.now() / 1000),
                },
              },
              value: {
                doubleValue: value,
              },
            },
          ],
        },
      ],
    };
    
    await monitoring.createTimeSeries(request);
    logger.debug('Custom metric written', { metric_type: metricType, value: value });
  } catch (error) {
    logger.error('Failed to write custom metric', {
      metric_type: metricType,
      error_message: error.message,
      error_type: 'custom_metric_error'
    });
  }
}

// Main Pub/Sub push endpoint
app.post('/pubsub', async (req, res) => {
  let idempotencyKey;
  let envelope;
  const startTime = Date.now();
  
  try {
    const message = req.body.message;
    if (!message || !message.data) {
      // NEW for Milestone C: Structured logging for invalid message format
      logger.warn('Invalid Pub/Sub message format received', {
        error_type: 'invalid_message_format',
        has_message: !!message,
        has_data: !!(message && message.data)
      });
      return res.status(400).json({ error: 'Invalid Pub/Sub message format' });
    }
    
    envelope = JSON.parse(Buffer.from(message.data, 'base64').toString());
    
    validateEnvelope(envelope);
    idempotencyKey = computeIdempotencyKey(envelope);
    
    // NEW for Milestone C: Enhanced structured logging with only IDs and reasons
    const baseLogData = {
      tenant_id: envelope.tenant_id,
      event_type: envelope.event_type,
      trace_id: envelope.trace_id,
      idempotency_key: idempotencyKey,
    };
    
    const sampled = shouldSample(idempotencyKey, AUDIT_RATE);
    
    // NEW for Milestone C: Log sampling decision
    logger.info('Event processed for sampling', {
      ...baseLogData,
      sampled: sampled,
      audit_rate: AUDIT_RATE
    });
    
    // NEW for Milestone C: Write sampling rate metric
    await writeCustomMetric('sampling_rate', sampled ? 1 : 0, {
      tenant_id: envelope.tenant_id,
      event_type: envelope.event_type
    });
    
    if (!sampled) {
      logger.info('Event not sampled, acknowledging without storage', {
        ...baseLogData,
        sampled: false
      });
      return res.status(204).send();
    }
    
    const processedPayload = processPayload(envelope.payload);
    
    const rowToInsert = {
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
      insertId: idempotencyKey,
    };
    
    // BigQuery write operation
    await bigquery
      .dataset(BQ_DATASET)
      .table(BQ_TABLE)
      .insert([rowToInsert]);

    const processingTime = Date.now() - startTime;
    
    // NEW for Milestone C: Enhanced success logging
    logger.info('BigQuery insert successful', {
      ...baseLogData,
      sampled: true,
      insert_status: 'SUCCESS',
      processing_time_ms: processingTime
    });
    
    // NEW for Milestone C: Write success metric
    await writeCustomMetric('bigquery_inserts', 1, {
      status: 'success',
      tenant_id: envelope.tenant_id,
      event_type: envelope.event_type
    });
    
    // Return success acknowledgement
    res.status(204).send();
    
  } catch (error) {
    const processingTime = Date.now() - startTime;
    
    // NEW for Milestone C: Enhanced error logging with structured data
    const errorLogData = {
      tenant_id: envelope?.tenant_id,
      event_type: envelope?.event_type,
      trace_id: envelope?.trace_id,
      idempotency_key: idempotencyKey,
      error_message: error.message,
      processing_time_ms: processingTime,
    };
    
    // Distinguish between terminal (4xx) and transient (5xx) errors
    if (error.message.includes('Missing required fields') || 
        error.message.includes('No idempotency key') ||
        error.message.includes('occurred_at must be a valid')) {
      
      // NEW for Milestone C: Terminal error logging
      logger.warn('Terminal validation error - sending to DLQ', {
        ...errorLogData,
        error_type: 'validation_error',
        insert_status: 'TERMINAL_ERROR'
      });
      
      // NEW for Milestone C: Write terminal error metric
      await writeCustomMetric('bigquery_inserts', 1, {
        status: 'terminal_error',
        tenant_id: envelope?.tenant_id || 'unknown',
        event_type: envelope?.event_type || 'unknown'
      });
      
      return res.status(400).json({ error: error.message });
    }
    
    // Transient errors (5xx)
    logger.error('Transient processing error - will retry', {
      ...errorLogData,
      error_type: 'transient_error',
      insert_status: 'TRANSIENT_ERROR',
      stack_trace: error.stack
    });
    
    // NEW for Milestone C: Write transient error metric
    await writeCustomMetric('bigquery_inserts', 1, {
      status: 'transient_error',
      tenant_id: envelope?.tenant_id || 'unknown',
      event_type: envelope?.event_type || 'unknown'
    });
    
    return res.status(503).json({ error: 'Internal processing error, please retry' });
  }
});

// --- SERVER START & SHUTDOWN ---

// NEW for Milestone C: Graceful shutdown with enhanced logging
process.on('SIGTERM', () => {
  logger.info('Received SIGTERM, shutting down gracefully', {
    uptime_seconds: Math.floor(process.uptime())
  });
  
  server.close((err) => {
    if (err) {
      logger.error('Error during server shutdown', { error: err.message });
      process.exit(1);
    } else {
      logger.info('Server shutdown completed gracefully');
      process.exit(0);
    }
  });
});

// Capture the server instance for testing
const server = app.listen(PORT, () => {
  logger.info('ETL Service started', { 
    port: PORT,
    service_name: SERVICE_NAME,
    audit_rate: AUDIT_RATE,
    bq_dataset: BQ_DATASET,
    bq_table: BQ_TABLE,
    default_phone_region: DEFAULT_PHONE_REGION,
    node_version: process.version,
    environment: process.env.NODE_ENV || 'development'
  });
});

// Export modules for testing
module.exports = { 
  validateEnvelope, 
  computeIdempotencyKey, 
  shouldSample, 
  normalizePhone, 
  processPayload, 
  writeCustomMetric,
  server,
  logger
};