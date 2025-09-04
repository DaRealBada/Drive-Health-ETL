// Use the v1 clients for direct API calls
const { v1 } = require('@google-cloud/pubsub');

// Create clients for both subscribing (reading) and publishing (writing)
const subscriberClient = new v1.SubscriberClient();
const publisherClient = new v1.PublisherClient();

// Configuration from environment variables
const DLQ_SUBSCRIPTION_NAME = process.env.DLQ_SUBSCRIPTION || 'call-audits-dlq-sub';
const MAIN_TOPIC_ID = process.env.MAIN_TOPIC || 'phone-call-metadata';
const PARKING_LOT_TOPIC_ID = process.env.PARKING_LOT_TOPIC || 'phone-call-metadata-parking-lot';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE) || 10;
const REPLAY_DELAY_MS = parseInt(process.env.REPLAY_DELAY_MS) || 200;

async function main() {
  console.log(`Starting DLQ replay job for subscription: ${DLQ_SUBSCRIPTION_NAME}`);

  const projectId = await subscriberClient.getProjectId();

  const formattedSubscription = `projects/${projectId}/subscriptions/${DLQ_SUBSCRIPTION_NAME}`;
  const formattedTopic = `projects/${projectId}/topics/${MAIN_TOPIC_ID}`;
  const formattedParkingLotTopic = `projects/${projectId}/topics/${PARKING_LOT_TOPIC_ID}`;

  // 1. Pull messages from the DLQ
  const pullRequest = {
    subscription: formattedSubscription,
    maxMessages: BATCH_SIZE,
  };
  const [response] = await subscriberClient.pull(pullRequest);
  const messages = response.receivedMessages;

  if (!messages || messages.length === 0) {
    console.log('No messages in DLQ to replay.');
    return;
  }

  console.log(`Found ${messages.length} messages to process.`);
  const ackIds = [];
  const messagesToRepublish = [];
  const messagesToPark = [];

  for (const { message, ackId } of messages) {
    if (message.attributes && message.attributes.replayAttempt) {
      // This message failed after a replay, send it to the parking lot
      console.log('Detected a stubborn message, sending to parking lot.');
      message.attributes.dlqReason = 'Failed after replay attempt';
      delete message.attributes.replayAttempt;
      messagesToPark.push(message);
    } else {
      // This is the first time replaying this message. Tag and republish.
      if (!message.attributes) {
        message.attributes = {};
      }
      message.attributes.replayAttempt = '1';
      messagesToRepublish.push(message);
    }
    ackIds.push(ackId);
  }

  // 2. Republish the messages to the main topic with throttling
  if (messagesToRepublish.length > 0) {
    console.log(`Re-publishing ${messagesToRepublish.length} messages to ${MAIN_TOPIC_ID}...`);
    for (const message of messagesToRepublish) {
      const publishRequest = {
        topic: formattedTopic,
        messages: [message],
      };
      await publisherClient.publish(publishRequest);
      console.log(`- Republished message to main topic.`);
      // Add a small delay to throttle the replay rate
      await new Promise(resolve => setTimeout(resolve, REPLAY_DELAY_MS));
    }
  }
  
  // 3. Publish stubborn messages to the parking-lot topic
  if (messagesToPark.length > 0) {
    const parkRequest = {
      topic: formattedParkingLotTopic,
      messages: messagesToPark,
    };
    await publisherClient.publish(parkRequest);
    console.log(`Parked ${messagesToPark.length} messages in ${PARKING_LOT_TOPIC_ID}.`);
  }
  
  // 4. Acknowledge the messages on the DLQ to remove them
  if (ackIds.length > 0) {
    const ackRequest = {
      subscription: formattedSubscription,
      ackIds: ackIds,
    };
    await subscriberClient.acknowledge(ackRequest);
    console.log(`Acknowledged ${ackIds.length} messages from the DLQ.`);
  }
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});

module.exports = { main };