// scripts/02_idempotency_test.js
// Milestone D: Duplicate collapse verification - proves insertId = idempotency_key works
const { PubSub } = require('@google-cloud/pubsub');

class IdempotencyTest {
  constructor(options = {}) {
    this.pubsub = new PubSub();
    this.topicName = options.topicName || 'phone-call-metadata';
    this.duplicateCount = options.duplicateCount || 5;
    this.results = {
      testCases: [],
      totalPublished: 0,
      totalErrors: 0,
      startTime: null,
      endTime: null
    };
  }

  createTestCases() {
    const timestamp = new Date().toISOString();
    const testId = Date.now();
    
    return [
      {
        name: 'Call ID Idempotency',
        idempotencyKey: `call-idem-${testId}-001`,
        envelope: {
          envelope_version: 1,
          event_type: 'call.metadata',
          schema_version: 1,
          tenant_id: 'org-idempotency-test',
          occurred_at: timestamp,
          trace_id: `idem-trace-${testId}-001`,
          source: 'idempotency-test',
          payload: {
            call_id: `call-idem-${testId}-001`,
            caller: '+14155550001',
            callee: '+14155550002',
            duration: 123,
            status: 'completed'
          }
        }
      },
      {
        name: 'Message ID Idempotency',
        idempotencyKey: `msg-idem-${testId}-002`,
        envelope: {
          envelope_version: 1,
          event_type: 'chat.message',
          schema_version: 1,
          tenant_id: 'org-idempotency-test',
          occurred_at: timestamp,
          trace_id: `idem-trace-${testId}-002`,
          source: 'idempotency-test',
          payload: {
            message_id: `msg-idem-${testId}-002`,
            from_phone: '+14155550003',
            to_phone: '+14155550004',
            channel: 'sms',
            text_length: 50
          }
        }
      },
      {
        name: 'Trace ID Fallback Idempotency',
        idempotencyKey: `trace-idem-${testId}-003`,
        envelope: {
          envelope_version: 1,
          event_type: 'call.metadata',
          schema_version: 1,
          tenant_id: 'org-idempotency-test',
          occurred_at: timestamp,
          trace_id: `trace-idem-${testId}-003`,
          source: 'idempotency-test',
          payload: {
            // No call_id or message_id - should fall back to trace_id
            caller: '+14155550005',
            callee: '+14155550006',
            duration: 67,
            status: 'completed'
          }
        }
      }
    ];
  }

  async publishDuplicates(testCase) {
    const topic = this.pubsub.topic(this.topicName);
    const caseResults = {
      name: testCase.name,
      idempotencyKey: testCase.idempotencyKey,
      published: 0,
      errors: 0,
      duplicateCount: this.duplicateCount
    };
    
    console.log(`\n📤 Testing: ${testCase.name}`);
    console.log(`   Idempotency Key: ${testCase.idempotencyKey}`);
    console.log(`   Publishing ${this.duplicateCount} identical messages...`);
    
    for (let i = 1; i <= this.duplicateCount; i++) {
      try {
        const data = Buffer.from(JSON.stringify(testCase.envelope));
        await topic.publishMessage({ data });
        caseResults.published++;
        console.log(`   📩 ${i}/${this.duplicateCount} - Published`);
        
        // Small delay to avoid overwhelming the service
        await new Promise(resolve => setTimeout(resolve, 200));
        
      } catch (error) {
        caseResults.errors++;
        console.error(`   ❌ ${i}/${this.duplicateCount} - Error:`, error.message);
      }
    }
    
    console.log(`   ✅ Published ${caseResults.published} duplicates (${caseResults.errors} errors)`);
    return caseResults;
  }

  async run() {
    console.log('🔄 Starting Idempotency Test - Duplicate Message Collapse');
    console.log(`📡 Publishing to topic: ${this.topicName}`);
    console.log(`🔢 Each test case will publish ${this.duplicateCount} identical messages`);
    console.log('🎯 Expected result: Exactly 1 row per test case in BigQuery');
    
    this.results.startTime = Date.now();
    const testCases = this.createTestCases();
    
    for (const testCase of testCases) {
      const caseResult = await this.publishDuplicates(testCase);
      this.results.testCases.push(caseResult);
      this.results.totalPublished += caseResult.published;
      this.results.totalErrors += caseResult.errors;
    }
    
    this.results.endTime = Date.now();
    
    // Wait for processing before showing verification queries
    console.log('\n⏳ Waiting 60 seconds for message processing...');
    await new Promise(resolve => setTimeout(resolve, 60000));
    
    this.printResults();
    this.generateVerificationQueries();
    this.generateReport();
  }

  printResults() {
    const duration = (this.results.endTime - this.results.startTime) / 1000;
    const totalExpectedMessages = this.results.testCases.length * this.duplicateCount;
    const publishSuccessRate = (this.results.totalPublished / totalExpectedMessages) * 100;
    
    console.log('\n🎯 ===== IDEMPOTENCY TEST RESULTS =====');
    console.log(`📊 Test cases: ${this.results.testCases.length}`);
    console.log(`