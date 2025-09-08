This document explains the process for using the Dead Letter Queue (DLQ) and the robust replay job to handle and recover messages that fail to be processed by the ETL service.

Overview
When a message fails processing due to a terminal error (e.g., a malformed envelope with missing required fields), the Cloud Run service returns a 4xx status code. After a configured number of delivery attempts, Pub/Sub automatically forwards this "poison message" to the Dead Letter Queue (DLQ) to prevent it from blocking the main pipeline.

The Replay Process
The replay process is a controlled workflow to recover messages from the DLQ. The provided src/replay-dlq-job.js script is a production-ready job designed for this purpose.

Pull from DLQ: The script runs in a loop, pulling batches of messages from the call-audits-dlq-sub subscription.

Tag and Retry: For each message, the script adds a x-replay-attempts attribute to track how many times it has been replayed. It then republishes the message to the original phone-call-metadata topic for another processing attempt.

Acknowledge Individually: The script only acknowledges a message from the DLQ after it has been successfully republished. This prevents data loss if the replay job itself encounters an error.

Parking Lot: The script is configured with a MAX_REPLAY_ATTEMPTS threshold (e.g., 3). If a message is pulled from the DLQ and its x-replay-attempts counter has reached this limit, the script will not retry it. Instead, it will move the message to a final parking-lot topic (phone-call-metadata-parking-lot) for manual engineering review and then acknowledge it from the DLQ. This prevents infinite retry loops for messages with persistent issues.

How to Run the Replay Job
The replay job is a standalone script and is not part of the deployed service. It can be run manually from the command line.

Ensure you are authenticated and have the correct permissions (the dlq-replay-sa service account is designed for this).

Execute the script using npm:

Bash

npm run replay
The script will run in a continuous loop, processing the DLQ in batches until the queue is empty.