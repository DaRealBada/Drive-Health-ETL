// Import the functions we want to test
const { validateEnvelope, normalizePhone } = require('./index.js');

// Group tests for the validateEnvelope function
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
    expect(validateEnvelope(validEnvelope)).toBe(true);
  });

  test('should throw an error for missing required fields', () => {
    const invalidEnvelope = { envelope_version: 1 };
    // Expecting the function to throw an error that contains this specific text
    expect(() => validateEnvelope(invalidEnvelope)).toThrow('Missing required fields');
  });

  test('should throw an error for an invalid timestamp', () => {
    const invalidEnvelope = { ...validEnvelope, occurred_at: 'not-a-date' };
    expect(() => validateEnvelope(invalidEnvelope)).toThrow('must be a valid ISO date string');
  });
});

// Group tests for the normalizePhone function
describe('normalizePhone', () => {
  test('should normalize a US phone number to E.164', () => {
    expect(normalizePhone('415-555-0001', 'US')).toBe('+14155550001');
  });

  test('should normalize a UK phone number to E.164', () => {
    expect(normalizePhone('020 7123 4567', 'GB')).toBe('+442071234567');
  });

  test('should return null for an invalid phone number', () => {
    expect(normalizePhone('not a phone number')).toBeNull();
  });
});