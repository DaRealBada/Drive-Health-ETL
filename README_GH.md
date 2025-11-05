# DriveHealth ETL Service

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Status: In Development](https://img.shields.io/badge/Status-In%20Development-yellow)](https://shields.io/)

A serverless application that ingests messages from Pub/Sub, transforms the data, and loads it into BigQuery, ensuring idempotency, scalability, and reliability.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Modules](#modules)
- [Monitoring](#monitoring)
- [DLQ and Replay Mechanism](#dlq-and-replay-mechanism)
- [Load Testing](#load-testing)
- [Configuration](#configuration)
- [License](#license)

## Overview

The DriveHealth ETL service is designed to efficiently process phone call metadata from Pub/Sub and store it in BigQuery. It's built with a focus on security, reliability, and data quality. Key features include:

*   **Idempotency:** Prevents duplicate records in BigQuery.
*   **Sampling:** Deterministic sampling of messages for auditing.
*   **Data Transformation:** Normalizes phone numbers to the E.164 format.
*   **Error Handling:** Sends messages to a Dead Letter Queue (DLQ) for terminal errors and triggers retries for transient errors.
*   **Scalability:** Leverages Cloud Run for automatic scaling.

## Architecture

The service follows a Pub/Sub Push model, where messages are automatically pushed to the Cloud Run service. The transformed data is stored in a BigQuery table partitioned by date and clustered by tenant and event type for optimal query performance.

**Message Flow:**

Pub/Sub -> Cloud Run (`/pubsub` endpoint) -> Validation -> Idempotency & Sampling -> Data Transformation -> BigQuery

## Modules

This project is structured into several modules, each responsible for a specific aspect of the ETL process:

*   **`Dockerfile`:** Defines the Docker image for the service, ensuring a consistent and reproducible environment.
*   **`app.js`:** The main application file, defining the Express.js server and API endpoints.
*   **`handler.js`:** Handles incoming Pub/Sub messages, orchestrating the validation, transformation, and loading processes.
*   **`bq.js`:** Manages writing event data to BigQuery in batches, ensuring idempotency.
*   **`batchProcessor.js`:** Implements the micro-batching mechanism for BigQuery inserts, improving efficiency.
*   **`load-test.js`:** A load testing tool for simulating high message volumes to assess the service's performance.
*   **`src/`:** Contains the source code for the application, organized into logical modules.
*   **`validation.js`, `phone.js`, `sampling.js`, `logger.js`:** Supporting modules for validation, phone number formatting, sampling logic, and logging.

## Monitoring

The `monitoring.md` file describes the monitoring and alerting setup for the DriveHealth ETL service using Google Cloud Monitoring. Key metrics include:

*   `Cloud Run: Request Count` (broken down by HTTP status codes)
*   `Cloud Run: p95 Latency`
*   `Cloud Run: Instance Count`
*   `Pub/Sub: DLQ Backlog`
*   `Log-based Metric: BigQuery Streaming Insert Errors`
*   `Log-based Metric: Observed Sampling Rate` (calculated as a ratio of sampled events to total events)

Alerting policies are in place for:

*   `High 5xx Error Rate`
*   `DLQ Backlog Growing`
*   `No Successful BigQuery Writes`

A custom log-based metric, "Observed Sampling Rate," is implemented to verify the configured `AUDIT_RATE`.

## DLQ and Replay Mechanism

The service utilizes a Dead Letter Queue (DLQ) and a replay mechanism to handle failed messages. The `dlq-replay.md` file describes the process for recovering messages from the DLQ and retrying them. Key components include:

*   **`call-audits-dlq-sub`:** The Pub/Sub subscription representing the DLQ.
*   **`phone-call-metadata`:** The original Pub/Sub topic where messages are initially published.
*   **`phone-call-metadata-parking-lot`:** The Pub/Sub topic where messages that exceed the maximum retry attempts are sent for manual review.
*   **`src/replay-dlq-job.js`:** The script responsible for implementing the replay logic.

The `03_dlq_and_replay.js` script is used to test and demonstrate the DLQ and replay mechanism.

## Load Testing

The `load-test.js` script is designed to perform a load test on the Pub/Sub topic. It generates a large number of messages, publishes them to the specified topic concurrently, and then reports the results of the test.

To run the load test:

1.  Configure the necessary environment variables (e.g., `PUBSUB_TOPIC_NAME`).
2.  Run the script: `node load-test.js`

The script will output the results of the load test, including the number of messages sent, errors, duration, and throughput.

## Configuration

The service relies on environment variables for configuration. Some key variables include:

*   `BQ_DATASET`: The BigQuery dataset name.
*   `BQ_TABLE`: The BigQuery table name.
*   `MAX_BATCH_SIZE`: The maximum number of messages to batch before writing to BigQuery.
*   `MAX_BATCH_WAIT_MS`: The maximum time to wait before flushing the batch.
*   `DLQ_SUBSCRIPTION_NAME`: The name of the DLQ subscription.
*   `MAIN_TOPIC_ID`: The name of the main topic.
*   `PARKING_LOT_TOPIC_ID`: The name of the parking lot topic.
*   `REPLAY_DELAY_MS`: Delay in milliseconds between replaying messages.
*   `MAX_REPLAY_ATTEMPTS`: The maximum number of replay attempts.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
