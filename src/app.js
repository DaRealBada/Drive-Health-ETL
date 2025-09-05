// src/app.js

// Import the required libraries
const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');

// The main handler logic is now in a separate file
const { handlePubSubRequest } = require('./handler');
const { ensureTable, ensureDataset } = require('./bq');

// Create the Express app
const app = express();
// Create the BigQuery client instance
const bigquery = new BigQuery();

// Configuration from environment variables
const PORT = process.env.PORT || 8080;
const BQ_DATASET = process.env.BQ_DATASET || 'drivehealth_dw';
const BQ_TABLE = process.env.BQ_TABLE || 'events';

// Configures express app to accept json
app.use(express.json());

// General server health check
app.get('/', (req, res) => {
  res.send('DriveHealth ETL Service is running!');
});

// Main Pub/Sub push endpoint
app.post('/pubsub', async (req, res) => {
  await handlePubSubRequest(req, res);
});


// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('Received SIGTERM, shutting down gracefully');
  process.exit(0);
});

module.exports = app;

async function bootServer() {
  await ensureDataset();
  await ensureTable();
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  })
}

if (require.main === module) {
  bootServer().catch(console.error);
}