// scripts/01_smoke_publish.js
// Milestone D: End-to-end sanity test with E.164 phone number verification
const { PubSub } = require('@google-cloud/pubsub');

class SmokeTest {
  constructor(options = {}) {
    this.pubsub = new PubSub();
    this.topicName = options.topicName || 'phone-call-metadata';
    this.testMessages = this.createTestMessages();
    this.results = {
      published: 0,
      errors: 0,
      startTime: null,
      endTime: null
    };
  }

  createTestMessages() {
    const timestamp = new Date().toISOString();
    const testId = Date.now();
    
    return [
      {
        name: 'US Phone Numbers',
        envelope: {
          envelope_version: 1,
          event_type: 'call.metadata',
          schema_version: 1,
          tenant_id: 'org-smoke-test',
          occurred_at: timestamp,
          trace_id: `smoke-${testId}-001`,
          source: 'smoke',
          payload: {
            call_id: `smoke-call-${testId}-001`,
            caller: '(415) 555-0001', // Should normalize to +14155550001
            callee: '415-555-0002',   // Should normalize to +14155550002
            duration: 123,
            status: 'completed'
          }
        }
      },
      {
        name: 'International Phone Numbers',
        envelope: {
          envelope_version: 1,
          event_type: 'call.metadata',
          schema_version: 1,
          tenant_id: 'org-smoke-test',
          occurred_at: timestamp,
          trace_id: `smoke-${testId}-002`,
          source: 'smoke',
          payload: {
            call_id: `smoke-call-${testId}-002`,
            caller: '+44 20 7123 4567', // UK number - should stay +442071234567
            callee: '+1 555 123 4567',  // US number - should become +15551234567
            duration: 45,
            status: 'completed'
          }
        }
      },
      {
        name: 'Mixed Message Types',
        envelope: {
          envelope_version: 1,
          event_type: 'chat.message',
          schema_version: 1,
          tenant_id: 'org-smoke-test',
          occurred_at: timestamp,
          trace_id: `smoke-${testId}-003`,
          source: 'smoke',
          payload: {
            message_id: `smoke-msg-${testId}-003`,
            from_phone: '555.123.4567', // Should normalize to +15551234567
            to_phone: '+1-555-987-6543', // Should normalize to +15559876543
            channel: 'sms',
            text_length: 42
          }
        }
      }
    ];
  }

  async publishMessages() {
    console.log('🧪 Starting Smoke Test - End-to-end sanity check');
    console.log(`📡 Publishing to topic: ${this.topicName}`);
    console.log(`📱 Testing E.164 phone normalization`);
    
    this.results.startTime = Date.now();
    const topic = this.pubsub.topic(this.topicName);
    
    for (const testCase of this.testMessages) {
      try {
        console.log(`\n📤 Publishing: ${testCase.name}`);
        console.log(`   Call ID: ${testCase.envelope.payload.call_id || testCase.envelope.payload.message_id}`);
        
        // Log original phone numbers for comparison
        if (testCase.envelope.payload.caller) {
          console.log(`   Original caller: ${testCase.envelope.payload.caller}`);
        }
        if (testCase.envelope.payload.callee) {
          console.log(`   Original callee: ${testCase.envelope.payload.callee}`);
        }
        if (testCase.envelope.payload.from_phone) {
          console.log(`   Original from_phone: ${testCase.envelope.payload.from_phone}`);
        }
        if (testCase.envelope.payload.to_phone) {
          console.log(`   Original to_phone: ${testCase.envelope.payload.to_phone}`);
        }
        
        const data = Buffer.from(JSON.stringify(testCase.envelope));
        await topic.publishMessage({ data });
        
        this.results.published++;
        console.log(`   ✅ Published successfully`);
        
        // Small delay between messages
        await new Promise(resolve => setTimeout(resolve, 100));
        
      } catch (error) {
        this.results.errors++;
        console.error(`   ❌ Error publishing ${testCase.name}:`, error.message);
      }
    }
    
    this.results.endTime = Date.now();
  }

  printResults() {
    const duration = (this.results.endTime - this.results.startTime) / 1000;
    
    console.log('\n🎯 ===== SMOKE TEST RESULTS =====');
    console.log(`📊 Messages published: ${this.results.published}/${this.testMessages.length}`);
    console.log(`❌ Errors: ${this.results.errors}`);
    console.log(`⏱️  Duration: ${duration.toFixed(2)}s`);
    console.log(`${this.results.errors === 0 ? '✅' : '❌'} Status: ${this.results.errors === 0 ? 'PASSED' : 'FAILED'}`);
    
    console.log('\n🔍 Next Steps:');
    console.log('1. Wait 30-60 seconds for processing');
    console.log('2. Check Cloud Run dashboard for 2xx requests');
    console.log('3. Verify data in BigQuery with E.164 normalization:');
    console.log(`
   SELECT 
     JSON_EXTRACT_SCALAR(payload, '$.call_id') as call_id,
     JSON_EXTRACT_SCALAR(payload, '$.message_id') as message_id,
     JSON_EXTRACT_SCALAR(payload, '$.caller') as normalized_caller,
     JSON_EXTRACT_SCALAR(payload, '$.callee') as normalized_callee,
     JSON_EXTRACT_SCALAR(payload, '$.from_phone') as normalized_from_phone,
     JSON_EXTRACT_SCALAR(payload, '$.to_phone') as normalized_to_phone,
     received_at
   FROM drivehealth_dw.events 
   WHERE source = 'smoke' 
   AND DATE(occurred_at) = CURRENT_DATE()
   ORDER BY received_at DESC;
    `);
    
    console.log('\n📱 Expected E.164 Normalization:');
    console.log('   (415) 555-0001  →  +14155550001');
    console.log('   415-555-0002   →  +14155550002');
    console.log('   +44 20 7123 4567  →  +442071234567');
    console.log('   555.123.4567   →  +15551234567');
    console.log('   +1-555-987-6543  →  +15559876543');
    
    console.log('\n4. Verify no errors in BigQuery streaming inserts');
    console.log('5. Confirm DLQ is empty (no failed messages)');
  }

  async run() {
    try {
      await this.publishMessages();
      this.printResults();
      
      // Generate a simple test report
      const report = {
        testType: 'Smoke Test',
        timestamp: new Date().toISOString(),
        results: this.results,
        passed: this.results.errors === 0 && this.results.published === this.testMessages.length,
        testCases: this.testMessages.map(tc => ({
          name: tc.name,
          event_type: tc.envelope.event_type,
          idempotency_key: tc.envelope.payload.call_id || tc.envelope.payload.message_id
        }))
      };
      
      const reportPath = `smoke-test-report-${Date.now()}.json`;
      require('fs').writeFileSync(reportPath, JSON.stringify(report, null, 2));
      console.log(`\n📄 Test report saved to: ${reportPath}`);
      
    } catch (error) {
      console.error('💥 Smoke test failed with unexpected error:', error);
      throw error;
    }
  }
}

// Command line interface
async function main() {
  const args = process.argv.slice(2);
  const options = {};
  
  // Parse command line arguments
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i]?.replace('--', '');
    const value = args[i + 1];
    
    switch (key) {
      case 'topic':
        options.topicName = value;
        break;
    }
  }
  
  const smokeTest = new SmokeTest(options);
  await smokeTest.run();
}

if (require.main === module) {
  main().catch(console.error);
}

module.exports = { SmokeTest };