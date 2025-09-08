# DriveHealth ETL Service

This document provides a comprehensive overview of the DriveHealth ETL service, detailing its architecture, message flow, and key configurations.

## Architecture

This service is built on a **serverless** architecture, designed to be highly scalable and resilient. It operates on a **Pub/Sub Push** model, where incoming messages are automatically and securely forwarded to a **Cloud Run** service. The core principles of the architecture are:

  * **Idempotency**: The service ensures that processing the same message multiple times results in only a single record being created in the data warehouse.
  * **Scalability**: The Cloud Run service automatically scales its instances to handle varying loads, as demonstrated by the load tests.
  * **Reliability**: A Dead Letter Queue (DLQ) and a robust replay job are in place to handle and recover from message failures.

## Infrastructure Setup

This setup follows security best practices by using separate service accounts for each component (Principle of Least Privilege) and enforces authenticated, secure invocations via OIDC.

### 1\. Create Service Accounts

First, create three dedicated service accounts for each part of the pipeline. This separation of duties ensures that each component only has the permissions it absolutely needs.

```sh
# Runtime SA for the Cloud Run service
gcloud iam service-accounts create runtime-sa --display-name="Runtime SA for ETL Service"

# Push SA for Pub/Sub to securely invoke Cloud Run
gcloud iam service-accounts create push-sa --display-name="Pub/Sub Push SA for ETL Service"

# DLQ Replay SA for the replay job
gcloud iam service-accounts create dlq-replay-sa --display-name="DLQ Replay Job SA"
```

### 2\. Grant IAM Permissions

Next, grant the specific, limited roles to each service account.

  * **Grant Runtime SA permissions** (to write to BigQuery and Logs):

    ```sh
    gcloud projects add-iam-policy-binding your-gcp-project-id \
      --member="serviceAccount:runtime-sa@your-gcp-project-id.iam.gserviceaccount.com" \
      --role="roles/bigquery.dataEditor"

    gcloud projects add-iam-policy-binding your-gcp-project-id \
      --member="serviceAccount:runtime-sa@your-gcp-project-id.iam.gserviceaccount.com" \
      --role="roles/logging.logWriter"
    ```

  * **Grant Push SA permission** (to invoke the Cloud Run service):

    ```sh
    gcloud run services add-iam-policy-binding your-etl-service --region=your-region \
      --member="serviceAccount:push-sa@your-gcp-project-id.iam.gserviceaccount.com" \
      --role="roles/run.invoker"
    ```

  * **Grant DLQ Replay SA permissions** (to read from DLQ and write to topics):

    ```sh
    gcloud pubsub subscriptions add-iam-policy-binding call-audits-dlq-sub \
      --member="serviceAccount:dlq-replay-sa@your-gcp-project-id.iam.gserviceaccount.com" \
      --role="roles/pubsub.subscriber"

    gcloud pubsub topics add-iam-policy-binding phone-call-metadata \
      --member="serviceAccount:dlq-replay-sa@your-gcp-project-id.iam.gserviceaccount.com" \
      --role="roles/pubsub.publisher"

    gcloud pubsub topics add-iam-policy-binding phone-call-metadata-parking-lot \
      --member="serviceAccount:dlq-replay-sa@your-gcp-project-id.iam.gserviceaccount.com" \
      --role="roles/pubsub.publisher"
    ```

### 3\. Create BigQuery Resources

  * **Create the Dataset**:

    ```sh
    bq --location=US mk --dataset \
    --description="Dataset for the DriveHealth ETL service" \
    your-gcp-project-id:drivehealth_dw
    ```

  * **Create the Table**:
    This schema is partitioned by date and clustered by tenant and event type for optimal query performance. The version fields are correctly typed as `INT64`.

    ```sh
    bq mk --table \
    --time_partitioning_field occurred_at \
    --time_partitioning_expiration 31536000 \
    --clustering_fields tenant_id,event_type \
    --description "Table for all incoming ETL events" \
    your-gcp-project-id:drivehealth_dw.events \
    tenant_id:STRING,event_type:STRING,schema_version:INT64,envelope_version:INT64,trace_id:STRING,occurred_at:TIMESTAMP,received_at:TIMESTAMP,source:STRING,sampled:BOOLEAN,idempotencyKey:STRING,payload:JSON
    ```

### 4\. Create Pub/Sub Topics and Secure Subscription

  * **Create the main topic**:
    ```sh
    gcloud pubsub topics create phone-call-metadata
    ```
  * **Create the DLQ topic**:
    ```sh
    gcloud pubsub topics create call-audits-dlq
    ```
  * **Create the Parking Lot topic**:
    ```sh
    gcloud pubsub topics create phone-call-metadata-parking-lot
    ```
  * **Create the secure push subscription**:
    This command configures the subscription to securely invoke your Cloud Run service using the Push SA (OIDC) and sets the DLQ topic.
    ```sh
    gcloud pubsub subscriptions create call-etl-sub \
      --topic=phone-call-metadata \
      --push-endpoint="YOUR_CLOUD_RUN_SERVICE_URL" \
      --push-auth-service-account="push-sa@your-gcp-project-id.iam.gserviceaccount.com" \
      --dead-letter-topic=call-audits-dlq
    ```

### 5\. Deploy the Secure Cloud Run Service

Deploy the service, specifying the **Runtime SA** and **disallowing unauthenticated access** to enforce the secure OIDC configuration.

```sh
gcloud run deploy your-etl-service \
  --source . \
  --platform managed \
  --region your-region \
  --service-account="runtime-sa@your-gcp-project-id.iam.gserviceaccount.com" \
  --no-allow-unauthenticated \
  --concurrency 250 \
  --min-instances 1 \
  --max-instances 10
```

## Data Flow & Processing

A single message follows a clearly defined path from ingestion to storage:

1.  **Ingestion**: The Cloud Run service receives a raw message from Pub/Sub at its `/pubsub` endpoint. The main logic is orchestrated by `handler.js`.
2.  **Validation**: The `validation.js` module ensures all required fields are present and correctly formatted.
3.  **Idempotency & Sampling**: A unique `idempotencyKey` is computed and used by `sampling.js` for deterministic sampling and by BigQuery's `insertId` to prevent duplicates.
4.  **Data Transformation**: The `phone.js` module normalizes phone numbers to the E.164 format.
5.  **Storage**: The `bq.js` module writes the final message as a single row into the BigQuery table.
6.  **Error Handling**: The service returns a `4xx` status for terminal errors (sending the message to the DLQ) and a `5xx` status for transient errors (triggering a retry).

## Envelope Specification

| Field              | Type   | Required | Description                                                          |
| :----------------- | :----- | :------- | :------------------------------------------------------------------- |
| `envelope_version` | number | Yes      | The version of the envelope schema.                                  |
| `event_type`       | string | Yes      | The type of event (e.g., "call.metadata").                           |
| `schema_version`   | number | Yes      | The version of the payload schema.                                   |
| `tenant_id`        | string | Yes      | The identifier for the customer/tenant.                              |
| `occurred_at`      | string | Yes      | ISO 8601 timestamp of when the event occurred.                       |
| `trace_id`         | string | No       | Unique ID to trace the request. Fallback for the idempotency key.      |
| `source`           | string | No       | The system that originated the event.                                |
| `payload`          | object | Yes      | The core data of the event. Must contain a `call_id` or `message_id`. |

## Environment Variables

| Variable             | Description                                                              | Default                         |
| :------------------- | :----------------------------------------------------------------------- | :------------------------------ |
| `PORT`               | The port the service will listen on.                                     | `8080`                          |
| `LOG_LEVEL`          | The logging level (e.g., 'info', 'warn', 'error').                       | `info`                          |
| `AUDIT_RATE`         | The deterministic sampling rate (0.0 to 1.0) for auditing.               | `1.0`                           |
| `DEFAULT_PHONE_REGION` | The default two-letter country code for E.164 phone number parsing.    | `US`                            |
| `MAX_BATCH_SIZE`     | The number of messages to buffer before flushing to BigQuery.            | `1`                             |
| `MAX_BATCH_WAIT_MS`  | The time in milliseconds to wait before flushing a batch.                | `100`                           |
| `BQ_DATASET`         | The BigQuery dataset to use.                                             | `drivehealth_dw`                |
| `BQ_TABLE`           | The BigQuery table to use.                                               | `events`                        |
| `DLQ_SUBSCRIPTION`   | The name of the Dead Letter Queue subscription.                          | `call-audits-dlq-sub`           |
| `MAIN_TOPIC`         | The name of the main Pub/Sub topic.                                      | `phone-call-metadata`           |
| `PARKING_LOT_TOPIC`  | The name of the parking-lot Pub/Sub topic for unrecoverable messages.    | `phone-call-metadata-parking-lot` |