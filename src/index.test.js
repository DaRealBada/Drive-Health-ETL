// Import all the functions and the server we want to test
const { validateEnvelope, computeIdempotencyKey } = require('./validation.js');
const { shouldSample } = require('./sampling.js');
const { normalizePhone } = require('./phone.js');
const { app } = require('./app.js');

describe('validateEnvelope', () => {
  const validEnvelope = {
    envelope_version: 1,
    event_type: 'call.metadata',
    schema_version: 1,
    tenant_id: 'org-demo',
    occurred_at: '2025-08-28T12:00:00Z',
    payload: { call_id: 'call-001' }
  };

  test('should return true for a valid envelope', () => {
    expect(() => validateEnvelope(validEnvelope)).not.toThrow();
  });

  test('should throw an error for missing required fields', () => {
    const invalidEnvelope = { envelope_version: 1, payload: {} };
    expect(() => validateEnvelope(invalidEnvelope)).toThrow('Missing required fields');
  });

  test('should throw an error for an invalid timestamp', () => {
    const invalidEnvelope = { ...validEnvelope, occurred_at: 'not-a-date' };
    expect(() => validateEnvelope(invalidEnvelope)).toThrow('must be a valid ISO date string');
  });
});

describe('normalizePhone', () => {
  test('should normalize a US phone number to E.164', () => {
    expect(normalizePhone('415-555-0001', 'US')).toBe('+14155550001');
  });

  test('should return null for an invalid phone number', () => {
    expect(normalizePhone('not a phone number')).toBeNull();
  });
});

describe('computeIdempotencyKey', () => {
  test('should prioritize call_id', () => {
    const envelope = { payload: { call_id: 'call123', message_id: 'msg123' }, trace_id: 'trace123' };
    expect(computeIdempotencyKey(envelope)).toBe('call123');
  });

  test('should use message_id if call_id is missing', () => {
    const envelope = { payload: { message_id: 'msg123' }, trace_id: 'trace123' };
    expect(computeIdempotencyKey(envelope)).toBe('msg123');
  });

  test('should use trace_id as a last resort', () => {
    const envelope = { payload: {}, trace_id: 'trace123' };
    expect(computeIdempotencyKey(envelope)).toBe('trace123');
  });

  test('should throw an error if no key is available', () => {
    const envelope = { payload: {} };
    expect(() => computeIdempotencyKey(envelope)).toThrow('No idempotency key found');
  });
});

describe('shouldSample', () => {
  test('should always return true when auditRate is 1.0', () => {
    expect(shouldSample('any-key', 1.0)).toBe(true);
  });

  test('should always return false when auditRate is 0.0', () => {
    expect(shouldSample('any-key', 0.0)).toBe(false);
  });

  test('should produce a deterministic (consistent) result', () => {
    const key = 'test-idempotency-key-123';
    const rate = 0.5;
    const firstResult = shouldSample(key, rate);
    const secondResult = shouldSample(key, rate);
    // The result should be the same every time for the same key and rate
    expect(firstResult).toBe(secondResult);
  });
});

// Server integration tests
describe('Server Integration', () => {
  let server;

  beforeAll((done) => {
    // Use port 0 to let the system assign an available port
    server = app.listen(0, done);
  }, 10000); // Increase timeout to 10 seconds

  afterAll((done) => {
    if (server) {
      server.close(done);
    } else {
      done();
    }
  });

  test('should respond to health check', async () => {
    const request = require('supertest');
    const response = await request(app).get('/');
    expect(response.status).toBe(200);
    expect(response.text).toBe('DriveHealth ETL Service is running!');
  });
});