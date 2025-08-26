// Opens express framework toolkit, BigQuery and PubSub, making it available to the code.
const express = require('express'); 
const { BigQuery } = require('@google-cloud/bigquery');
const { PubSub } = require('@google-cloud/pubsub');
// Configures node.js app to use express framework. Creates an instance of the express application.
const app = express();
// Checks if Cloud environment (env) tells us a port (listens to incoming requests) to use, if not the port defaults to 8080.
const PORT = process.env.PORT || 8080;
// Sets up a BigQuery variable
const bigquery = new BigQuery();
// Sets up a PubSub variable
const pubSubClient = new PubSub();
// Middleware which parses incoming JSON requests into a valid JavaScript to work with.
app.use(express.json());


// GET service acts as a general server health check. Uses the default/root path ('/').
app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

// POST service receives a scheduled trigger, pulls a batch, and processes it.
app.post('/process-call', async (req, res) => {
  // 1. DEFINE a subscription and pull request.
  const subscriptionName = 'call-audits-pull-subscription';
  const pullRequest = {
    maxMessages: 100, // The maximum number of messages to pull in one go.
  };

  try {
    console.log('Scheduled trigger received. Pulling messages...');

    // 2. PULL a batch of messages from the subscription.
    const [messages] = await pubSubClient.subscription(subscriptionName).getMessages(pullRequest);

    if (messages.length === 0) {
      console.log('No messages to process.');
      res.status(200).send('No messages to process.');
      return;
    }

    console.log(`Pulled ${messages.length} messages.`);

    // 3. PREPARE an array to hold the rows for BigQuery.
    const rowsToInsert = [];
    const ackIds = []; // Array to hold IDs of messages to acknowledge.

    // 4. LOOP through each message in the batch.
    for (const message of messages) {
      // Add the message's ackId to our list for later.
      ackIds.push(message.ackId);

      // The same logic as before, but for each message.
      const callData = JSON.parse(Buffer.from(message.message.data, 'base64').toString());

      if (callData.call_id) {
        if (Math.random() < 0.05) { // Reset to 5%
          const row = {
            call_id: callData.call_id,
            timestamp: callData.timestamp,
            duration: callData.duration,
            flagged_for_audit: true,
            processed_at: new Date().toISOString(),
            original_metadata: JSON.stringify(callData) 
          };
          rowsToInsert.push(row); // Add the processed row to our batch array.
        }
      }
    }

    // 5. INSERT the entire batch into BigQuery if there's anything to insert.
    if (rowsToInsert.length > 0) {
      await bigquery.dataset('call_audits').table('processed_calls').insert(rowsToInsert);
      console.log(`Successfully inserted ${rowsToInsert.length} rows into BigQuery.`);
    }

    // 6. ACKNOWLEDGE all pulled messages so Pub/Sub doesn't send them again.
    if (ackIds.length > 0) {
      await pubSubClient.subscription(subscriptionName).acknowledge(ackIds);
      console.log('Acknowledged all processed messages.');
    }

    res.status(200).send(`Processed ${messages.length} messages.`);
  } catch (error) {
    console.error('Error processing batch:', error);
    res.status(500).send('Error processing batch');
  }
});

// Turns on server and makes it listen for incoming requests on specified port.
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});