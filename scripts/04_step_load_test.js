// scripts/04_step_load_test.js
// Milestone D: Step load test with autoscaling demonstration
const { PubSub } = require('@google-cloud/pubsub');
const { LoadTester } = require('../load-test');

class StepLoadTester {
  constructor(options = {}) {
    this.pubsub = new PubSub();
    this.topicName = options.topicName || 'phone-call-metadata';
    this.testSteps = options.testSteps || [
      { name: 'Warm-up', messagesPerMinute: 50, durationMinutes: 2 },
      { name: 'Low Load', messagesPerMinute: 100, durationMinutes: 5 },
      { name: 'Medium Load', messagesPerMinute: 500, durationMinutes: 5 },
      { name: 'High Load', messagesPerMinute: 1000, durationMinutes: 5 },
      { name: 'Cool-down', messagesPerMinute: 100, durationMinutes: 2 }
    ];
    this.tenantIds = options.tenantIds || ['org-demo', 'org-test', 'org-prod'];
    this.eventTypes = options.eventTypes || ['call.metadata', 'call.summary'];
    
    this.results = {
      steps: [],
      totalMessages: 0,
      totalErrors: 0,
      startTime: null,
      endTime: null
    };
  }

  generateTestMessage(stepName, messageIndex, globalIndex) {
    const tenantId = this.tenantIds[globalIndex % this.tenantIds.length];
    const eventType = this.eventTypes[globalIndex % this.eventTypes.length];
    const timestamp = new Date().toISOString();
    
    return {
      envelope_version: 1,
      event_type: eventType,
      schema_version: 1,
      tenant_id: tenantId,
      occurred_at: timestamp,
      trace_id: `step-load-${stepName}-${globalIndex}-${Date.now()}`,
      source: 'step-load-test',
      payload: {
        call_id: `step-load-call-${stepName}-${globalIndex}-${Date.now()}`,
        caller: this.generatePhoneNumber(),
        callee: this.generatePhoneNumber(),
        duration: Math.floor(Math.random() * 300) + 10,
        status: Math.random() > 0.05 ? 'completed' : 'failed', // 95% success rate
        metadata: {
          test: true,
          step: stepName,
          sequence: messageIndex,
          global_sequence: globalIndex
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

  async publishAtRate(stepName, messagesPerMinute, durationMinutes, globalMessageIndex) {
    const intervalMs = 60000 / messagesPerMinute; // Convert rate to interval
    const totalMessages = messagesPerMinute * durationMinutes;
    const endTime = Date.now() + (durationMinutes * 60 * 1000);
    
    console.log(`\n📊 Starting step: ${stepName}`);
    console.log(`   Rate: ${messagesPerMinute} messages/minute`);
    console.log(`   Duration: ${durationMinutes} minutes`);
    console.log(`   Interval: ${intervalMs.toFixed(1)}ms between messages`);
    console.log(`   Expected total: ${totalMessages} messages`);
    
    const stepResults = {
      stepName,
      messagesPerMinute,
      durationMinutes,
      sent: 0,
      errors: 0,
      startTime: Date.now(),
      endTime: null,
      actualRate: 0
    };
    
    let messageIndex = 0;
    const topic = this.pubsub.topic(this.topicName);
    
    // Use setInterval for precise timing
    return new Promise((resolve) => {
      const interval = setInterval(async () => {
        if (Date.now() >= endTime) {
          clearInterval(interval);
          stepResults.endTime = Date.now();
          stepResults.actualRate = (stepResults.sent / ((stepResults.endTime - stepResults.startTime) / 60000));
          
          console.log(`   ✅ Completed: ${stepResults.sent}/${totalMessages} messages`);
          console.log(`   📈 Actual rate: ${stepResults.actualRate.toFixed(1)} messages/minute`);
          console.log(`   ❌ Errors: ${stepResults.errors}`);
          
          resolve(stepResults);
          return;
        }
        
        try {
          const message = this.generateTestMessage(stepName, messageIndex, globalMessageIndex.value);
          const data = Buffer.from(JSON.stringify(message));
          
          await topic.publishMessage({ data });
          stepResults.sent++;
          globalMessageIndex.value++;
          
          // Progress indicator every 100 messages
          if (stepResults.sent % 100 === 0) {
            const elapsed = (Date.now() - stepResults.startTime) / 60000;
            const currentRate = stepResults.sent / elapsed;
            console.log(`   📤 Sent ${stepResults.sent} messages (${currentRate.toFixed(1)}/min)`);
          }
          
        } catch (error) {
          stepResults.errors++;
          console.error(`   ❌ Error publishing message ${messageIndex}:`, error.message);
        }
        
        messageIndex++;
      }, intervalMs);
    });
  }

  async run() {
    console.log('🚀 Starting Step Load Test for DriveHealth ETL');
    console.log(`📡 Target topic: ${this.topicName}`);
    console.log(`👥 Tenants: ${this.tenantIds.join(', ')}`);
    console.log(`📋 Event types: ${this.eventTypes.join(', ')}`);
    console.log(`⏱️  Total test duration: ${this.testSteps.reduce((sum, step) => sum + step.durationMinutes, 0)} minutes`);
    
    this.results.startTime = Date.now();
    const globalMessageIndex = { value: 0 }; // Use object for reference passing
    
    // Run each step sequentially
    for (const step of this.testSteps) {
      const stepResult = await this.publishAtRate(
        step.name,
        step.messagesPerMinute,
        step.durationMinutes,
        globalMessageIndex
      );
      
      this.results.steps.push(stepResult);
      this.results.totalMessages += stepResult.sent;
      this.results.totalErrors += stepResult.errors;
      
      // Brief pause between steps
      console.log('   ⏸️  Pausing 30 seconds before next step...');
      await new Promise(resolve => setTimeout(resolve, 30000));
    }
    
    this.results.endTime = Date.now();
    this.printFinalResults();
    this.generateReport();
  }

  printFinalResults() {
    const totalDuration = (this.results.endTime - this.results.startTime) / 60000;
    const overallRate = this.results.totalMessages / totalDuration;
    const errorRate = (this.results.totalErrors / this.results.totalMessages) * 100;
    
    console.log('\n🎯 ===== STEP LOAD TEST RESULTS =====');
    console.log(`📊 Total messages: ${this.results.totalMessages}`);
    console.log(`⏱️  Total duration: ${totalDuration.toFixed(2)} minutes`);
    console.log(`📈 Overall rate: ${overallRate.toFixed(1)} messages/minute`);
    console.log(`❌ Total errors: ${this.results.totalErrors}`);
    console.log(`📉 Error rate: ${errorRate.toFixed(3)}%`);
    console.log(`${errorRate < 0.5 ? '✅' : '❌'} Error rate target: < 0.5% ${errorRate < 0.5 ? 'PASSED' : 'FAILED'}`);
    
    console.log('\n📋 Step-by-step breakdown:');
    this.results.steps.forEach(step => {
      const duration = (step.endTime - step.startTime) / 60000;
      const stepErrorRate = (step.errors / step.sent) * 100;
      console.log(`   ${step.stepName}: ${step.sent} msgs (${step.actualRate.toFixed(1)}/min, ${stepErrorRate.toFixed(3)}% errors)`);
    });
    
    console.log('\n🔍 Next Steps:');
    console.log('1. Check your Cloud Monitoring dashboard for:');
    console.log('   - Cloud Run instance scaling behavior');
    console.log('   - Request latency (P95 should be < 1s)');
    console.log('   - 2xx/4xx/5xx distribution');
    console.log('   - Pub/Sub delivery metrics');
    console.log('2. Verify BigQuery data ingestion:');
    console.log(`   SELECT tenant_id, event_type, COUNT(*) FROM drivehealth_dw.events WHERE source = 'step-load-test' GROUP BY 1, 2`);
    console.log('3. Check for any messages in DLQ (should be near zero)');
  }

  generateReport() {
    const report = {
      testType: 'Step Load Test',
      timestamp: new Date().toISOString(),
      configuration: {
        topic: this.topicName,
        tenants: this.tenantIds,
        eventTypes: this.eventTypes,
        steps: this.testSteps
      },
      results: this.results,
      metrics: {
        totalDurationMinutes: (this.results.endTime - this.results.startTime) / 60000,
        overallRate: this.results.totalMessages / ((this.results.endTime - this.results.startTime) / 60000),
        errorRate: (this.results.totalErrors / this.results.totalMessages) * 100,
        passed: (this.results.totalErrors / this.results.totalMessages) < 0.005 // < 0.5%
      }
    };
    
    const reportPath = `load-test-report-${Date.now()}.json`;
    require('fs').writeFileSync(reportPath, JSON.stringify(report, null, 2));
    console.log(`\n📄 Detailed report saved to: ${reportPath}`);
  }
}

// Command line interface
async function main() {
  const args = process.argv.slice(2);
  const options = {};
  
  // Parse command line arguments
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i].replace('--', '');
    const value = args[i + 1];
    
    switch (key) {
      case 'topic':
        options.topicName = value;
        break;
      case 'tenants':
        options.tenantIds = value.split(',');
        break;
      case 'types':
        options.eventTypes = value.split(',');
        break;
      case 'quick':
        // Quick test mode for development
        options.testSteps = [
          { name: 'Quick-Low', messagesPerMinute: 60, durationMinutes: 1 },
          { name: 'Quick-High', messagesPerMinute: 180, durationMinutes: 1 }
        ];
        break;
    }
  }
  
  const tester = new StepLoadTester(options);
  await tester.run();
}

if (require.main === module) {
  main().catch(console.error);
}

module.exports = { StepLoadTester };