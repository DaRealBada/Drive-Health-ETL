// src/handler.test.js

const { processPubSubMessage } = require('./handler');

// --- Mocks ---
jest.mock('./bq', () => ({
  writeBatchToBigQuery: jest.fn(),
}));
jest.mock('./validation', () => ({
  validateAndExtractKey: jest.fn(),
}));
jest.mock('./sampling', () => ({
  evaluateSampling: jest.fn().mockReturnValue(true),
}));
jest.mock('./phone', () => ({
  processPayload: jest.fn(payload => payload), // Return payload unmodified
}));
// --- End Mocks ---

const { writeBatchToBigQuery } = require('./bq');
const { validateAndExtractKey } = require('./validation');

describe('processPubSubMessage', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  test('should return logMetadata in single-message mode', async () => {
    // Mock a successful validation and BQ write that includes logMetadata
    validateAndExtractKey.mockReturnValue({ isValid: true, idempotencyKey: 'single-key' });
    writeBatchToBigQuery.mockResolvedValue({
      success: true,
      count: 1,
      processingTime: 50,
      logMetadata: {
        batch_size: 1,
        processing_time_ms: 50,
        insert_status: 'BATCH_SUCCESS', // This is what we want to verify
      },
    });

    // Create a sample Pub/Sub message
    const message = {
      data: Buffer.from(JSON.stringify({ tenant_id: 'test' })).toString('base64'),
    };

    // Run the function and get the result
    const result = await processPubSubMessage(message);

    // --- Assertions ---
    // 1. Check that the overall process was successful
    expect(result.success).toBe(true);
    
    // 2. Verify that the logMetadata object was returned
    expect(result.logMetadata).toBeDefined();

    // 3. Verify a specific field within the metadata to confirm it's the correct object
    expect(result.logMetadata.insert_status).toBe('BATCH_SUCCESS');
  });
});