// test-bq.js
const { createBigQueryRow } = require('./src/bq');

// Sample valid message payload
const envelope = {
  "tenant_id": "org-demo",
  "event_type": "call.metadata",
  "schema_version": 1,
  "envelope_version": 1,
  "trace_id": "test-trace-id-123",
  "occurred_at": "2025-09-04T12:30:00.000Z",
  "source": "test-system",
  "payload": {
    "call_id": "call-67890-test",
    "caller": "+14155551234",
    "callee": "+14155555678",
    "duration": 180,
    "status": "completed",
    "message_id": null,
    "from_phone": null,
    "to_phone": null,
    "channel": null,
    "text_length": null,
    "metadata": "{\"call_quality\":\"good\",\"recording_enabled\":true,\"test_call\":true}"
  }
};

const processedPayload = envelope.payload;
const idempotencyKey = envelope.payload.call_id;

const bqRow = createBigQueryRow(envelope, processedPayload, idempotencyKey);

console.log(JSON.stringify(bqRow, null, 2));

// Log types for inspection
console.log("\n--- Data Types ---");
for (const key in bqRow) {
  if (typeof bqRow[key] === 'object' && bqRow[key] !== null) {
    for (const nestedKey in bqRow[key]) {
      console.log(`${key}.${nestedKey}: ${typeof bqRow[key][nestedKey]}`);
    }
  } else {
    console.log(`${key}: ${typeof bqRow[key]}`);
  }
}