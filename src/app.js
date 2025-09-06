// src/app.js

const express = require('express');
const { handlePubSubRequest } = require('./handler');
const { flushPendingBatch } = require('./batchProcessor'); 

const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json());

app.get('/', (req, res) => {
  res.send('DriveHealth ETL Service is running!');
});

app.post('/pubsub', handlePubSubRequest);

// fix: Graceful shutdown now flushes the pending batch.
process.on('SIGTERM', async () => {
  console.log('Received SIGTERM. Flushing pending batch...');
  try {
    await flushPendingBatch();
    console.log('Batch flushed. Shutting down gracefully.');
    process.exit(0);
  } catch (error) {
    console.error('Error during shutdown flush:', error);
    process.exit(1);
  }
});

module.exports = app;

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}