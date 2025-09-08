// src/index.js
// Robust DLQ replay job with proper error handling and message reconstruction

const { v1 } = require('@google-cloud/pubsub');
const subscriberClient = new v1.SubscriberClient();
const publisherClient = new v1.PublisherClient();

// --- Configuration ---
const DLQ_SUBSCRIPTION_NAME = process.env.DLQ_SUBSCRIPTION || 'call-audits-dlq-sub';
const MAIN_TOPIC_ID = process.env.MAIN_TOPIC || 'phone-call-metadata';
const PARKING_LOT_TOPIC_ID = process.env.PARKING_LOT_TOPIC || 'phone-call-metadata-parking-lot';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 10;
const REPLAY_DELAY_MS = parseInt(process.env.REPLAY_DELAY_MS) || 200;
const MAX_REPLAY_ATTEMPTS = parseInt(process.env.MAX_REPLAY_ATTEMPTS) || 3;
const MAX_PULLS = parseInt(process.env.MAX_PULLS) || 100; // Safety cap to prevent infinite loops

/**
 * Safely reconstruct a message for republishing.
 * This is a pure function that prepares a message for its next destination.
 */
function reconstructMessage(receivedMessage, isParking = false) {
  const { message } = receivedMessage;
  const currentAttempts = parseInt(message.attributes?.['x-replay-attempts'] || '0');
  const newAttemptCount = currentAttempts + 1;

  // Reconstruct the core message with data as a Buffer
  const reconstructed = {
    data: Buffer.from(message.data, 'base64'),
    attributes: {
      // Copy existing attributes, filtering out internal and old tracking attributes
      ...Object.fromEntries(
        Object.entries(message.attributes || {}).filter(([key]) => 
          !key.startsWith('googclient_') && key !== 'x-replay-attempts'
        )
      ),
      'x-original-message-id': message.messageId || 'unknown',
      'x-replay-timestamp': new Date().toISOString(),
    }
  };

  // Add specific attributes based on whether we are parking or replaying
  if (isParking) {
    reconstructed.attributes['x-parked-reason'] = `Exceeded max replay attempts (${MAX_REPLAY_ATTEMPTS})`;
    reconstructed.attributes['x-final-attempt-count'] = newAttemptCount.toString();
  } else {
    reconstructed.attributes['x-replay-attempts'] = newAttemptCount.toString();
  }

  if (message.orderingKey) {
    reconstructed.orderingKey = message.orderingKey;
  }

  return reconstructed;
}

/**
 * Pull and process a single batch of messages from the DLQ.
 */
async function pullAndProcessBatch(formattedSubscription, formattedTopic, formattedParkingLotTopic) {
  const [response] = await subscriberClient.pull({
    subscription: formattedSubscription,
    maxMessages: BATCH_SIZE,
  });

  const messages = response.receivedMessages;
  if (!messages || messages.length === 0) {
    return { messagesFound: 0, shouldContinue: false };
  }

  console.log(`Pulled ${messages.length} messages to process.`);
  
  const successfulAckIds = [];
  
  for (const receivedMessage of messages) {
    try {
      const currentAttempts = parseInt(receivedMessage.message.attributes?.['x-replay-attempts'] || '0');
      let messageToPublish;
      let targetTopic;
      let action;

      if (currentAttempts >= MAX_REPLAY_ATTEMPTS) {
        // Time to park the message
        console.log(`Message ${receivedMessage.message.messageId} exceeded max attempts, parking...`);
        messageToPublish = reconstructMessage(receivedMessage, true);
        targetTopic = formattedParkingLotTopic;
        action = 'parked';
      } else {
        // Re-attempt to publish to the main topic
        messageToPublish = reconstructMessage(receivedMessage, false);
        targetTopic = formattedTopic;
        action = 'republished';
      }

      await publisherClient.publish({ topic: targetTopic, messages: [messageToPublish] });
      console.log(` ✓ Message ${receivedMessage.message.messageId} successfully ${action}.`);
      
      // Add this message's ackId to the list of successful ones for this batch
      successfulAckIds.push(receivedMessage.ackId);

      if (REPLAY_DELAY_MS > 0) {
        await new Promise(resolve => setTimeout(resolve, REPLAY_DELAY_MS));
      }

    } catch (error) {
      console.error(` ✗ Failed to process message ${receivedMessage.message.messageId}. It will not be acknowledged.`, error);
    }
  }

  // Acknowledge all successfully processed messages from this batch at once
  if (successfulAckIds.length > 0) {
    await subscriberClient.acknowledge({ subscription: formattedSubscription, ackIds: successfulAckIds });
    console.log(`Acknowledged ${successfulAckIds.length} successfully processed messages.`);
  }

  return { messagesFound: messages.length, shouldContinue: true };
}

/**
 * Main DLQ replay function.
 */
async function main() {
  console.log(`Starting DLQ replay job for subscription: ${DLQ_SUBSCRIPTION_NAME}`);
  
  const projectId = await subscriberClient.getProjectId();
  const formattedSubscription = `projects/${projectId}/subscriptions/${DLQ_SUBSCRIPTION_NAME}`;
  const formattedTopic = `projects/${projectId}/topics/${MAIN_TOPIC_ID}`;
  const formattedParkingLotTopic = `projects/${projectId}/topics/${PARKING_LOT_TOPIC_ID}`;

  let pullCount = 0;
  while (pullCount < MAX_PULLS) {
    console.log(`\n--- Pull attempt ${pullCount + 1} of ${MAX_PULLS} ---`);
    const { messagesFound, shouldContinue } = await pullAndProcessBatch(
      formattedSubscription,
      formattedTopic,
      formattedParkingLotTopic
    );
    pullCount++;
    if (!shouldContinue) {
      console.log('DLQ is empty. Replay job complete.');
      break;
    }
  }

  if (pullCount >= MAX_PULLS) {
    console.warn(`⚠️ Reached maximum pull limit (${MAX_PULLS}). DLQ may still contain messages.`);
  }
}

if (require.main === module) {
  main().catch(e => {
    console.error('An unhandled error occurred in the main process:', e);
    process.exit(1);
  });
}

module.exports = { main };