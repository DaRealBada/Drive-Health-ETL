This document explains the process for using the Dead Letter Queue (DLQ) and replay job to handle messages that fail to be processed by the ETL service.

Overview
The ETL service is configured with a Dead Letter Policy. When a message fails to be processed after a specified number of delivery attempts (e.g., 5), it is automatically forwarded to the Dead Letter Queue. These messages often represent "terminal" errors that cannot be resolved by simple retries, such as a malformed envelope with missing required fields.

Replay Process
The replay process is a manual or automated workflow to recover messages from the DLQ and attempt re-ingestion into the main pipeline. The provided 

index.js script serves as the replay job for this purpose.


Pull from DLQ: The index.js script pulls messages from the DLQ subscription (call-audits-dlq-sub).

Repair (Optional): Messages from the DLQ can be manually inspected and corrected if the error is due to a data issue. The replay script itself doesn't perform correction, but it can be modified to do so. In this implementation, the 03_dlq_and_replay.js test script is used to simulate a malformed message being pushed to the DLQ and then corrected and replayed.

Republish: The script republishes the messages to the original topic (phone-call-metadata).

Re-ingestion: The republish messages are processed by the ETL service as if they were new. This time, with the issue corrected, they should be successfully ingested into BigQuery.


Parking Lot: If a message fails again after being replayed (e.g., if the error was not fixed), it will be sent to a separate "parking-lot" topic for further investigation.

How to run the Replay Job
To manually run the replay job locally to process messages from the DLQ, you would use a command like this:
node index.js









