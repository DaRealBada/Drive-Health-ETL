// Opens express framework toolkit and BigQuery, making it available to the code.
const express = require('express'); 
const { BigQuery } = require('@google-cloud/bigquery');
// Configures node.js app to use express framework. Creates an instance of the express application.
const app = express();
// Checks if Cloud environment (env) tells us a port (listens to incoming requests) to use, if not the port defaults to 8080
const PORT = process.env.PORT || 8080;
// Sets up a BigQuery variable
const bigquery = new BigQuery();
// Middleware which parses incoming JSON requests into a valid JavaScript to work with.
app.use(express.json());


// GET service acts as a general test (no endpoint) which can be 
app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

app.post('/process-call', async (req, res) => {
  try {
    console.log('Received Pub/Sub message');

    const message = req.body.message;
    const callData = JSON.parse(Buffer.from(message.data, 'base64').toString());

    console.log('Decoded call data:', callData);

    // Use a higher percentage for easier testing, e.g., 1.0 for 100%
    if (Math.random() <= 1.0) { 
      console.log(`Call ${callData.call_id} SELECTED for audit`);

      const rowToInsert = {
        call_id: callData.call_id,
        timestamp: callData.timestamp,
        duration: callData.duration,
        flagged_for_audit: true,
        processed_at: new Date().toISOString(),
        original_metadata: JSON.stringify(callData)
      };

      const datasetId = 'call_audits'; // Your dataset ID
      const tableId = 'processed_calls';   // Your table ID

      await bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert([rowToInsert]);

      console.log(`Successfully inserted call ${callData.call_id} into BigQuery.`);

    } else {
      console.log(`Call ${callData.call_id} not selected for audit`);
    }

    res.status(200).send('Message processed');
  } catch (error) {
    console.error('Error processing message:', error);
    res.status(500).send('Error processing message');
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});