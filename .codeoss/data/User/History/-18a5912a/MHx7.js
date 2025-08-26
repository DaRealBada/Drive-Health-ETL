// Opens express framework toolkit and BigQuery, making it available to the code.
const express = require('express'); 
const { BigQuery } = require('@google-cloud/bigquery');
// Configures node.js app to use express framework. Creates an instance of the express application.
const app = express();
// Checks if Cloud environment (env) tells us a port (listens to incoming requests) to use, if not the port defaults to 8080.
const PORT = process.env.PORT || 8080;
// Sets up a BigQuery variable
const bigquery = new BigQuery();
// Middleware which parses incoming JSON requests into a valid JavaScript to work with.
app.use(express.json());


// GET service acts as a general server health check. Uses the default/root path ('/').
app.get('/', (req, res) => {
  res.send('ETL Service is running!');
});

// POST service receives request from Pub/Sub, returns a response. Includes endpoint for incoming requests.
app.post('/process-call', async (req, res) => {
  try {
    console.log('Received Pub/Sub message');
    // The request body has a message inside we need to open
    const message = req.body.message;
    // Takes the encoded message.data and tells Node.js, "This is a base64 container". Then, it opens and converts container into a regular JSON string.
    //  We then take this and parse it into a structured JavaScript Object
    const callData = JSON.parse(Buffer.from(message.data, 'base64').toString());

    console.log('Decoded call data:', callData);

    // Selects 100% (5%) of calls to be audited
    if (Math.random() <= 1.0) { 
      console.log(`Call ${callData.call_id} SELECTED for audit`);
      // Moving callData into rowToInsert. 
      const rowToInsert = {
        call_id: callData.call_id,
        timestamp: callData.timestamp,
        duration: callData.duration,
        // Added a flagged for audit boolean variable. 
        flagged_for_audit: true,
        // Added processed_at variable. Returns string representation of date and time
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

// Turns on server and makes it listen for incoming requests on specified port.
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});