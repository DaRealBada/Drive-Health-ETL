# DriveHealth ETL Service

This document provides a comprehensive overview of the DriveHealth ETL service, detailing its architecture, message flow, and key configurations.

## Prerequisites

Before running this service, you must manually create the necessary BigQuery resources.

### 1. Create the BigQuery Dataset

The service requires a dataset to store the event data. Run the following `gcloud` command to create it. Replace `your-gcp-project-id` with your actual project ID.

```sh
bq --location=US mk --dataset \
--description="Dataset for the DriveHealth ETL service" \
your-gcp-project-id:drivehealth_dw
````

### 2\. Create the BigQuery Table

Once the dataset is created, you must create the `events` table with the correct schema, partitioning, and clustering. This command also sets a partition expiration of 365 days.

```sh
bq mk --table \
--time_partitioning_field occurred_at \
--time_partitioning_expiration 31536000 \
--clustering_fields tenant_id,event_type \
--description "Table for all incoming ETL events" \
your-gcp-project-id:drivehealth_dw.events \
tenant_id:STRING,event_type:STRING,schema_version:STRING,envelope_version:STRING,trace_id:STRING,occurred_at:TIMESTAMP,received_at:TIMESTAMP,source:STRING,sampled:BOOLEAN,idempotencyKey:STRING,payload:JSON
```

## Infrastructure Setup

This service relies on specific Pub/Sub and Cloud Run configurations.

### Pub/Sub Configuration

You must create a main topic, a Dead-Letter Queue (DLQ) topic, and a push subscription that connects them to your Cloud Run service.

**1. Create the main topic:**

```sh
gcloud pubsub topics create phone-call-metadata
```

**2. Create the Dead-Letter Queue (DLQ) topic:**
This topic will receive messages that fail processing after all retry attempts.

```sh
gcloud pubsub topics create phone-call-metadata-dlq
```

**3. Create the push subscription:**
This command creates a subscription to the main topic, configures it to push messages to your Cloud Run URL, and sets the DLQ topic.

```sh
gcloud pubsub subscriptions create call-etl-sub \
  --topic=phone-call-metadata \
  --push-endpoint="YOUR_CLOUD_RUN_SERVICE_URL" \
  --dead-letter-topic=phone-call-metadata-dlq \
  --dead-letter-topic-project="your-gcp-project-id"
```

**Note**: Replace `YOUR_CLOUD_RUN_SERVICE_URL` and `your-gcp-project-id` with your values.

### Cloud Run Configuration

For optimal performance and cost-effectiveness in production, the following settings are recommended during deployment:

  * **Concurrency (`--concurrency`)**: A higher concurrency (e.g., `250`) is recommended. This service is I/O-bound (waiting on BigQuery), so a single instance can handle many concurrent requests efficiently.
  * **Autoscaling (`--min-instances` and `--max-instances`)**:
      * Set a minimum of `1` instance to reduce "cold starts" if you have consistent traffic.
      * Set a maximum number of instances (e.g., `10`) to control costs during large traffic spikes.

You can apply these settings during deployment with the following flags:

```sh
gcloud run deploy your-etl-service \
  --source . \
  --concurrency 250 \
  --min-instances 1 \
  --max-instances 10
```

## Architecture

This service is built on a **serverless** architecture, designed to be highly scalable and resilient. It operates on a **Pub/Sub Push** model, where incoming messages are automatically forwarded to a **Cloud Run** service. The core principles of the architecture are:

  * **Idempotency**: The service ensures that processing the same message multiple times results in only a single record being created in the data warehouse.
  * **Scalability**: The Cloud Run service automatically scales its instances to handle varying loads, as demonstrated by the load tests.
  * **Reliability**: A Dead Letter Queue (DLQ) and replay mechanism are in place to handle and recover from message failures.

## Data Flow & Processing

A single message follows a clearly defined path from ingestion to storage:

1.  **Ingestion**: The Cloud Run service receives a raw message from Pub/Sub at its `/pubsub` endpoint. The main logic is orchestrated by `handler.js`.
2.  **Validation**: The `validation.js` module ensures all required fields are present and correctly formatted.
3.  **Idempotency & Sampling**: A unique `idempotencyKey` is computed and used by `sampling.js` for deterministic sampling and by BigQuery's `insertId` to prevent duplicates.
4.  **Data Transformation**: The `phone.js` module normalizes phone numbers to the E.164 format.
5.  **Storage**: The `bq.js` module writes the final message as a single row into the BigQuery table.
6.  **Error Handling**: The service returns a `4xx` status for terminal errors (sending the message to the DLQ) and a `5xx` status for transient errors (triggering a retry).

## Envelope Specification

All incoming messages must conform to the following JSON envelope structure:

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `envelope_version` | number | Yes | The version of the envelope schema. |
| `event_type` | string | Yes | The type of event (e.g., "call.metadata"). |
| `schema_version` | number | Yes | The version of the payload schema. |
| `tenant_id` | string | Yes | The identifier for the customer/tenant. |
| `occurred_at` | string | Yes | ISO 8601 timestamp of when the event occurred. |
| `trace_id` | string | No | Unique ID to trace the request. Fallback for the idempotency key. |
| `source` | string | No | The system that originated the event. |
| `payload` | object | Yes | The core data of the event. Must contain a `call_id` or `message_id`. |

## Environment Variables

The service can be configured using these environment variables:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PORT` | The port the service will listen on. | `8080` |
| `LOG_LEVEL` | The logging level (e.g., 'info', 'warn', 'error'). | `info` |
| `AUDIT_RATE` | The deterministic sampling rate (0.0 to 1.0) for auditing. | `1.0` |
| `DEFAULT_PHONE_REGION` | The default two-letter country code for E.164 phone number parsing. | `US` |
| `MAX_BATCH_SIZE` | The number of messages to buffer before flushing to BigQuery. | `1` |
| `MAX_BATCH_WAIT_MS` | The time in milliseconds to wait before flushing a batch. | `100` |
| `BQ_DATASET` | The BigQuery dataset to use. | `drivehealth_dw` |
| `BQ_TABLE` | The BigQuery table to use. | `events` |
| `DLQ_SUBSCRIPTION` | The name of the Dead Letter Queue subscription. | `call-audits-dlq-sub`|
| `MAIN_TOPIC` | The name of the main Pub/Sub topic. | `phone-call-metadata`|
| `PARKING_LOT_TOPIC` | The name of the parking-lot Pub/Sub topic for unrecoverable messages. | `phone-call-metadata-parking-lot`|