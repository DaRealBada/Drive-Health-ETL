// Opens express framework toolkit.
const express = require('express');
// Opens the v1 Pub/Sub client for direct API calls.
const { v1 } = require('@google-cloud/pubsub');
// Opens the BigQuery client library.
const { BigQuery } = require('@google-cloud/bigquery');


// Creates an instance of the express application.
const app = express();
// Sets the port from the environment, defaulting to 8080.
const PORT = process.env.PORT || 8080;

// Creates a BigQuery client instance.
const bigquery = new BigQuery();
// Creates a Pub/Sub v1 client instance.
const pubSubClient = new v1.SubscriberClient();

// Sets up configuration from environment variables with defaults.
const SUBSCRIPTION_NAME = process.env.SUBSCRIPTION_NAME || 'call-audits-pull-subscription';
const DATASET_ID = process.env.DATASET_ID || 'call_audits';
const TABLE_ID = process.env.TABLE_ID || 'processed_calls';
const AUDIT_RATE = parseFloat(process.env.AUDIT_RATE) || 1; // should be change to 5% (0.05)
const MAX_MESSAGES = parseInt(process.env.MAX_MESSAGES) || 100;

// Middleware to parse incoming JSON requests.
app.use(express.json());

// Health check endpoint to confirm the service is running.
app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

// Main batch processing endpoint, triggered by Cloud Scheduler.
app.post('/process-call', async (req, res) => {
  try {
    console.log('Scheduled trigger received. Pulling messages...');
    
    // Gets the project ID from the environment where the code is running.
    const projectId = await pubSubClient.getProjectId();
    // Formats the full subscription path required by the API.
    const formattedSubscription = `projects/${projectId}/subscriptions/${SUBSCRIPTION_NAME}`;
    
    // Defines the request to pull a batch of messages.
    const pullRequest = {
      subscription: formattedSubscription,
      maxMessages: MAX_MESSAGES,
    };

    // Executes the pull request to get a batch of messages.
    const [response] = await pubSubClient.pull(pullRequest);
    // Extracts the array of messages from the response.
    const messages = response.receivedMessages;

    // If no messages are found, exit gracefully.
    if (!messages || messages.length === 0) {
      console.log('No messages to process.');
      return res.status(204).send(); // 204 No Content
    }

    console.log(`Pulled ${messages.length} messages.`);

    // Prepares an array for BigQuery rows and message acknowledgement IDs.
    const rowsToInsert = [];
    const ackIds = [];

    // Loops through each message in the pulled batch.
    for (const { message, ackId } of messages) {
      // Always add the message's ackId to be acknowledged at the end.
      ackIds.push(ackId);
      // Inner try/catch to handle errors for a single malformed message.
      try {
        // Decodes and parses the individual message data.
        const callData = JSON.parse(Buffer.from(message.data, 'base64').toString());

        // Validates that the required call_id field exists.
        if (!callData.call_id) {
          // If validation fails, logs a warning and skips to the next message.
          console.warn(`Message missing call_id, skipping. AckId: ${ackId}`);
          continue;
        }

        // Randomly samples messages based on the configured audit rate.
        if (Math.random() < AUDIT_RATE) {
          // Transforms the data into the format for BigQuery.
          const row = {
            call_id: callData.call_id,
            timestamp: callData.timestamp,
            duration: callData.duration,
            flagged_for_audit: true,
            processed_at: new Date().toISOString(),
            original_metadata: JSON.stringify(callData)
          };
          // Adds the prepared row to our batch array for BigQuery.
          rowsToInsert.push(row);
        }
      } catch (parseError) {
        // If the message is malformed, log the error and continue.
        console.error(`Error parsing message with ackId ${ackId}, skipping:`, parseError);
      }
    }

    // After the loop, check if there are any rows to insert.
    if (rowsToInsert.length > 0) {
        // Inserts the entire batch of rows into BigQuery in a single API call.
        await bigquery
          .dataset(DATASET_ID)
          .table(TABLE_ID)
          .insert(rowsToInsert);
        console.log(`Successfully inserted ${rowsToInsert.length} rows into BigQuery.`);
    }

    // After processing, acknowledge all pulled messages.
    if (ackIds.length > 0) {
      // Defines the request to acknowledge the messages.
      const ackRequest = {
        subscription: formattedSubscription,
        ackIds: ackIds,
      };
      // Sends the acknowledge request to Pub/Sub.
      await pubSubClient.acknowledge(ackRequest);
      console.log(`Acknowledged ${ackIds.length} processed messages.`);
    }

    // Sends a success response back to the scheduler.
    res.status(200).send(`Processed ${messages.length} messages.`);
    
  } catch (error) {
    // Main catch block for fatal errors during the batch process.
    console.error('Error processing batch:', error);
    res.status(500).send('Error processing batch');
  }
});

// Starts the server and listens for incoming requests on the specified port.
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});