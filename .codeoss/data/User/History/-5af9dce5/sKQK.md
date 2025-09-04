# DriveHealth ETL Service

This document provides a comprehensive overview of the DriveHealth ETL service, detailing its architecture, message flow, and key configurations.

## Architecture

This service is built on a **serverless** architecture, designed to be highly scalable and resilient. It operates on a **Pub/Sub Push** model, where incoming messages are automatically forwarded to a **Cloud Run** service. The core principles of the architecture are:

* **Idempotency**: The service ensures that processing the same message multiple times results in only a single record being created in the data warehouse.
* **Scalability**: The Cloud Run service automatically scales its instances to handle varying loads, as demonstrated by the load tests.
* **Reliability**: A Dead Letter Queue (DLQ) and replay mechanism are in place to handle and recover from message failures.

## Data Flow & Processing

A single message follows a clearly defined path from ingestion to storage:

1.  **Ingestion:** The Cloud Run service receives a raw message from Pub/Sub at its `/pubsub` endpoint. The main logic is orchestrated by `handler.js`, which manages the entire flow.
2.  **Validation:** The message envelope is parsed and validated by the `validation.js` module to ensure all required fields are present and correctly formatted.
3.  **Idempotency & Sampling:** A unique `idempotency_key` is computed from the message payload. This key is used by the `sampling.js` module to make a deterministic decision on whether to process or discard the message based on the `AUDIT_RATE`. It is also used as the `insertId` for BigQuery to prevent duplicate records.
4.  **Data Transformation:** The `phone.js` module processes the message payload to normalize phone numbers to the E.164 format, which standardizes the data before it is stored.
5.  **Storage:** The `bq.js` module is responsible for writing the final processed and validated message as a single row into the designated BigQuery table.
6.  **Error Handling:** In case of a processing failure, the service returns a specific HTTP status code to Pub/Sub. Terminal errors result in a `4xx` response, sending the message to the DLQ, while transient errors result in a `5xx` response, triggering a retry.

## Envelope Specification

All incoming messages must conform to the following JSON envelope structure:

| Field | Type | Required | Description |
| `envelope_version` | number | Yes | The version of the envelope schema. |
| `event_type` | string | Yes | The type of event (e.g., "call.metadata"). |
| `schema_version` | number | Yes | The version of the payload schema. |
| `tenant_id` | string | Yes | The identifier for the customer/tenant. |
| `occurred_at` | string | Yes | ISO 8601 timestamp of when  event occurred |
| `trace_id` | string | No | Unique ID to trace the request. Fallback for the idempotency key. |
| `source` | string | No | The system that originated the event. |
| `payload` | object | Yes | The core data of the event. Must contain a `call_id` or `message_id` for idempotency. |

## Environment Variables

The service can be configured using these environment variables:

| Variable | Description | Default |
|---|---|---|
| `PORT` | The port the service will listen on. | `8080` |
| `AUDIT_RATE` | The deterministic sampling rate (0.0 to 1.0) for auditing. | `1.0` |
| `DEFAULT_PHONE_REGION` | The default two-letter country code for E.164 phone number parsing. | `US` |