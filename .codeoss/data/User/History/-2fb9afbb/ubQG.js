const express = require('express');
const { v1 } = require('@google-cloud/pubsub');
const { BigQuery } = require('@google-cloud/bigquery');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 8080;

const bigquery = new BigQuery();
const pubSubClient = new v1.SubscriberClient();

// Configuration from environment variables
const SUBSCRIPTION_NAME = process.env.SUBSCRIPTION_NAME || 'etl-batch-pull-sub';
const DATASET_ID = process.env.DATASET_ID || 'call_audits';
const TABLE_ID = process.env.TABLE_ID || 'processed_calls';
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE) || 0.05;
const MAX_MESSAGES = parseInt(process.env.MAX_MESSAGES) || 100;

app.use(express.json());

app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

app.post('/process-call', async (req, res) => {
  try {
    const hashData = (data) => {
      const piiFields = ['caller', 'receiver'];
      const hashedData = { ...data };
      for (const field of piiFields) {
        if (hashedData[field]) {
          const hash = crypto.createHash('sha256').update(hashedData[field]);
          hashedData[field] = hash.digest('hex');
        }
      }
      return hashedData;
    };

    console.log('Scheduled trigger received. Pulling messages...');
    
    const projectId = await pubSubClient.getProjectId();
    const formattedSubscription = `projects/${projectId}/subscriptions/${SUBSCRIPTION_NAME}`;
    
    const pullRequest = {
      subscription: formattedSubscription,
      maxMessages: MAX_MESSAGES,
    };

    const [response] = await pubSubClient.pull(pullRequest);
    const messages = response.receivedMessages;

    if (!messages || messages.length === 0) {
      console.log('No messages to process.');
      return res.status(204).send();
    }

    console.log(`Pulled ${messages.length} messages.`);

    const rowsToInsert = [];
    const ackIds = [];

    for (const { message, ackId } of messages) {
      ackIds.push(ackId);
      try {
        const callData = JSON.parse(Buffer.from(message.data, 'base64').toString());

        if (!callData.call_id) {
          console.warn(`Message missing call_id, skipping. AckId: ${ackId}`);
          continue;
        }

        if (Math.random() < AUDIT_RATE) {
          const hashedCallData = hashData(callData);
          const row = {
            call_id: callData.call_id,
            timestamp: callData.timestamp,
            duration: callData.duration,
            flagged_for_audit: true,
            processed_at: new Date().toISOString(),
            original_metadata: JSON.stringify(hashedCallData)
          };
          rowsToInsert.push(row);
        }
      } catch (parseError) {
        console.error(`Error parsing message with ackId ${ackId}, skipping:`, parseError);
      }
    }

    if (rowsToInsert.length > 0) {
        await bigquery.dataset(DATASET_ID).table(TABLE_ID).insert(rowsToInsert);
        console.log(`Successfully inserted ${rowsToInsert.length} rows into BigQuery.`);
    }

    if (ackIds.length > 0) {
      const ackRequest = {
        subscription: formattedSubscription,
        ackIds: ackIds,
      };
      await pubSubClient.acknowledge(ackRequest);
      console.log(`Acknowledged ${ackIds.length} processed messages.`);
    }

    res.status(200).send(`Processed ${messages.length} messages.`);
    
  } catch (error) {
    console.error('Error processing batch:', error);
    res.status(500).send('Error processing batch');
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});