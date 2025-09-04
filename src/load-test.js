// load-test.js
const { PubSub } = require('@google-cloud/pubsub');

class LoadTester {
  constructor(options = {}) {
    this.pubsub = new PubSub();
    this.topicName = options.topicName || 'phone-call-metadata';
    this.concurrency = options.concurrency || 10;
    this.totalMessages = options.totalMessages || 1000;
    this.tenantIds = options.tenantIds || ['org-demo', 'org-test', 'org-load'];
    this.eventTypes = options.eventTypes || ['call.metadata', 'call.summary'];

    this.topic = this.pubsub.topic(this.topicName);
    this.results = {
      sent: 0,
      errors: 0,
      startTime: null,
      endTime: null
    };
  }

  generateTestMessage(index) {
    const tenantId = this.tenantIds[index % this.tenantIds.length];
    const eventType = this.eventTypes[index % this.eventTypes.length];
    const timestamp = new Date().toISOString();

    return {
      envelope_version: 1,
      event_type: eventType,
      schema_version: 1,
      tenant_id: tenantId,
      occurred_at: timestamp,
      trace_id: `load-test-trace-${index}-${Date.now()}`,
      source: 'load-test',
      payload: {
        call_id: `load-test-call-${index}-${Date.now()}`,
        caller: this.generatePhoneNumber(),
        callee: this.generatePhoneNumber(),
        duration: Math.floor(Math.random() * 300) + 10,
        status: Math.random() > 0.1 ? 'completed' : 'failed',
        metadata: {
          test: true,
          batch: Math.floor(index / 100),
          sequence: index
        }
      }
    };
  }

  generatePhoneNumber() {
    const areaCode = Math.floor(Math.random() * 800) + 200;
    const exchange = Math.floor(Math.random() * 800) + 200;
    const number = Math.floor(Math.random() * 9999).toString().padStart(4, '0');
    return `+1${areaCode}${exchange}${number}`;
  }

  async publishBatch(messages) {
    const publishPromises = messages.map(async (message, index) => {
      try {
        const data = Buffer.from(JSON.stringify(message));
        await this.topic.publishMessage({ data });
        this.results.sent++;

        if (this.results.sent % 100 === 0) {
          console.log(`Published ${this.results.sent}/${this.totalMessages} messages`);
        }

        return { success: true, index };
      } catch (error) {
        this.results.errors++;
        console.error(`Error publishing message ${index}:`, error.message);
        return { success: false, index, error: error.message };
      }
    });

    return Promise.allSettled(publishPromises);
  }

  async run() {
    console.log(`Starting load test: ${this.totalMessages} messages with ${this.concurrency} concurrent batches`);
    console.log(`Target topic: ${this.topicName}`);
    console.log(`Tenants: ${this.tenantIds.join(', ')}`);
    console.log(`Event types: ${this.eventTypes.join(', ')}`);

    this.results.startTime = Date.now();

    const batchSize = Math.ceil(this.totalMessages / this.concurrency);
    const batches = [];

    for (let i = 0; i < this.concurrency; i++) {
      const batchStart = i * batchSize;
      const batchEnd = Math.min(batchStart + batchSize, this.totalMessages);
      const batchMessages = [];

      for (let j = batchStart; j < batchEnd; j++) {
        batchMessages.push(this.generateTestMessage(j));
      }

      batches.push(batchMessages);
    }

    console.log(`Publishing ${batches.length} batches concurrently...`);

    const batchPromises = batches.map((batch, index) => {
      console.log(`Starting batch ${index + 1} with ${batch.length} messages`);
      return this.publishBatch(batch);
    });

    await Promise.all(batchPromises);

    this.results.endTime = Date.now();
    this.printResults();
  }

  printResults() {
    const duration = (this.results.endTime - this.results.startTime) / 1000;
    const messagesPerSecond = this.results.sent / duration;

    console.log('\n=== LOAD TEST RESULTS ===');
    console.log(`Messages sent: ${this.results.sent}/${this.totalMessages}`);
    console.log(`Errors: ${this.results.errors}`);
    console.log(`Success rate: ${((this.results.sent / this.totalMessages) * 100).toFixed(2)}%`);
    console.log(`Duration: ${duration.toFixed(2)}s`);
    console.log(`Throughput: ${messagesPerSecond.toFixed(2)} messages/second`);
    console.log('========================\n');

    if (this.results.errors > 0) {
      console.warn(`⚠️  ${this.results.errors} messages failed to publish`);
    } else {
      console.log('✅ All messages published successfully!');
    }

    console.log(`📊 Check your monitoring dashboard for processing metrics`);
    console.log(`🔍 Query BigQuery to verify data ingestion:`);
    console.log(`   SELECT tenant_id, event_type, COUNT(*) FROM drivehealth_dw.events WHERE source = 'load-test' GROUP BY 1, 2`);
  }
}

async function main() {
  const tester = new LoadTester();
  await tester.run();
}

if (require.main === module) {
  main().catch(console.error);
}

module.exports = { LoadTester };