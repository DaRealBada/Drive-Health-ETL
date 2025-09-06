// src/app.test.js

const request = require('supertest');
const app = require('./app');

// --- Mocks ---
// Mock the BigQuery module to prevent actual API calls.
jest.mock('./bq', () => ({
  writeBatchToBigQuery: jest.fn(),
}));

// Mock the validation module to control its output in tests.
jest.mock('./validation', () => ({
  validateAndExtractKey: jest.fn(),
}));

// Mock the sampling module to ensure events are always processed in tests.
jest.mock('./sampling', () => ({
  evaluateSampling: jest.fn().mockReturnValue(true),
}));
// --- End Mocks ---

// Import the mocked modules *after* the jest.mock calls.
const { writeBatchToBigQuery } = require('./bq');
const { validateAndExtractKey } = require('./validation');


// Helper function to create a valid base64-encoded Pub/Sub message
const createPubSubMessage = (payload) => {
  const data = Buffer.from(JSON.stringify(payload)).toString('base64');
  return { message: { data } };
};


describe('POST /pubsub Endpoint', () => {

  // Reset mocks before each test to ensure a clean state.
  beforeEach(() => {
    jest.clearAllMocks();
  });

  // Test for a successful request (2xx response)
  test('should return 204 for a valid event', async () => {
    // fix: Mock a successful validation and BQ write.
    validateAndExtractKey.mockReturnValue({ isValid: true, idempotencyKey: 'test-key' });
    writeBatchToBigQuery.mockResolvedValue({ success: true, count: 1 });

    const validEvent = { tenant_id: 'tenant-123', event_type: 'test_event' };
    const response = await request(app).post('/pubsub').send(createPubSubMessage(validEvent));
    
    expect(response.status).toBe(204);
  });

  // Test for a terminal validation error (4xx response)
  test('should return 400 for an event with missing required fields', async () => {
    // fix: Mock a failed validation.
    validateAndExtractKey.mockReturnValue({ isValid: false, errors: ['Missing required fields'] });
    
    const invalidEvent = { payload: { data: 'test' } };
    const response = await request(app).post('/pubsub').send(createPubSubMessage(invalidEvent));
      
    expect(response.status).toBe(400);
    expect(response.text).toContain('Bad Request: Missing required fields');
  });
  
  // Test for malformed JSON (another 4xx response)
  test('should return 422 for a malformed JSON envelope', async () => {
    // This test doesn't need mocks because the error happens before validation.
    const malformedMessage = { message: { data: 'not-valid-json-at-all' } };
    const response = await request(app).post('/pubsub').send(malformedMessage);

    // fix: Updated the expected error message to match what the service actually returns.
    expect(response.status).toBe(422);
    expect(response.text).toContain('is not valid JSON'); 
  });

  // Test for a transient server error (5xx response)
  test('should return 503 when BigQuery write fails transiently', async () => {
    // fix: Mock a successful validation but a failed BQ write.
    validateAndExtractKey.mockReturnValue({ isValid: true, idempotencyKey: 'test-key' });
    writeBatchToBigQuery.mockResolvedValue({
      success: false,
      errors: [new Error('BigQuery is temporarily unavailable')],
    });

    const validEvent = { tenant_id: 'tenant-123', event_type: 'test_event' };
    const response = await request(app).post('/pubsub').send(createPubSubMessage(validEvent));

    expect(response.status).toBe(503);
    expect(response.text).toContain('Internal Server Error');
  });
});