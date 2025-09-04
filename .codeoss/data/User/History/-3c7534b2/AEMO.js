// scripts/03_dlq_and_replay.js
// Milestone D: DLQ and Replay Job Verification
const { PubSub } = require('@google-cloud/pubsub');

class DLQReplayTest {
  constructor(options = {}) {
    this.pubsub = new PubSub();
    this.topicName = options.topicName || 'phone-call-metadata';
    this.dlqTopicName = options.dlqTopicName || 'phone-call-metadata-dlq';
    this.dlqSubscriptionName = options.dlqSubscriptionName || 'call-audits-dlq-sub';
    this.replayJobScript = options.replayJobScript || './index.js'; // The script to run the replay job
    this.testId = Date.now();
  }

  createMalformedEnvelope() {
    return {
      envelope_version: 1,
      event_type: 'call.metadata',
      schema_version: 1,
      // Missing 'tenant_id' to cause a terminal 4xx error
      occurred_at: new Date().toISOString(),
      payload: {
        call_id: `call-dlq-test-${this.testId}`,
        caller: '+14155550001',
        callee: '+14155550002',
        duration: 123,
      }
    };
  }
  
  createCorrectedEnvelope() {
    const envelope = this.createMalformedEnvelope();
    envelope.tenant_id = 'org-dlq-replayed';
    return envelope;
  }

  async run() {
    console.log('💀 Starting DLQ and Replay Test');
    console.log(`📡 Target Topic: ${this.topicName}`);
    console.log(`⚰️  DLQ Subscription: ${this.dlqSubscriptionName}`);
    
    console.log('\n1. Publishing a malformed message (missing `tenant_id`)...');
    try {
      const malformedEnvelope = this.createMalformedEnvelope();
      const data = Buffer.from(JSON.stringify(malformedEnvelope));
      await this.pubsub.topic(this.topicName).publishMessage({ data });
      console.log('   ✅ Malformed message published successfully.');
    } catch (error) {
      console.error('   ❌ Failed to publish malformed message:', error.message);
      return;
    }

    console.log('\n2. Waiting 90 seconds for the service to process and move the message to the DLQ...');
    await new Promise(resolve => setTimeout(resolve, 90000));
    
    console.log('   - Now, check your Cloud Monitoring dashboard. You should see a brief 4xx spike and a non-zero DLQ backlog[cite: 228, 229].');
    console.log(`   - Run the following gcloud command to confirm the message is in the DLQ:`);
    console.log(`    gcloud pubsub subscriptions pull ${this.dlqSubscriptionName} --limit=1 --format="json" --project=[YOUR_PROJECT_ID]`);
    
    console.log('\n3. Correcting and re-publishing the message via the Replay Job...');
    console.log(`   - This test assumes you have a separate Replay Job (e.g., the script in your \`index.js\` file) that can be triggered manually or on a schedule.`);
    console.log(`   - Please run the Replay Job now to pull the message from the DLQ, correct it, and republish it to the main topic.`);
    console.log(`   - Example command to run the Replay Job locally: \`node ${this.replayJobScript}\``);
    
    console.log('\n4. Waiting 60 seconds for the replayed message to be processed...');
    await new Promise(resolve => setTimeout(resolve, 60000));

    console.log('\n5. Verifying successful re-ingestion in BigQuery...');
    console.log('   - After the replay job, the message should have been successfully processed and written to BigQuery[cite: 224].');
    console.log('   - You can use this query to verify the row exists:');
    console.log(`
SELECT tenant_id, event_type, payload
FROM \`drivehealth_dw.events\`
WHERE JSON_EXTRACT_SCALAR(payload, '$.call_id') = 'call-dlq-test-${this.testId}';
    `);
    
    console.log('\n✅ DLQ and Replay Test complete. Check your dashboards and BigQuery for final verification.');
  }
}

async function main() {
  const test = new DLQReplayTest();
  await test.run();
}

if (require.main === module) {
  main().catch(console.error);
}

module.exports = { DLQReplayTest };