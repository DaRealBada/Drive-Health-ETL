# DriveHealth ETL Service

This is a serverless ETL service that processes call metadata for auditing purposes. It receives messages from a Pub/Sub push subscription, validates them, normalizes PII, and prepares the data for storage in BigQuery.

## Architecture

This service uses a **Pub/Sub Push** model. Each message is pushed to the `/pubsub` endpoint on a **Cloud Run** service, which processes the message individually.

The service is designed to be idempotent, meaning the same message can be processed multiple times with only one final record being created in the data warehouse.

## Envelope Specification

[cite_start]All incoming messages must conform to the following JSON envelope structure[cite: 102]:

| Field | Type | Required | Description |
|---|---|---|---|
| `envelope_version` | number | Yes | The version of the envelope schema. |
| `event_type` | string | Yes | The type of event (e.g., "call.metadata"). |
| `schema_version` | number | Yes | The version of the payload schema. |
| `tenant_id` | string | Yes | The identifier for the customer/organization. |
| `occurred_at` | string | Yes | ISO 8601 timestamp of when the event occurred. |
| `trace_id` | string | No | A unique ID for tracing the request through systems. Used as a fallback for the idempotency key. |
| `source` | string | No | The system that originated the event (e.g., "twilio"). |
| `payload` | object | Yes | The core data of the event. Must contain a `call_id` or `message_id` for idempotency. |


## Environment Variables

The service is configured using the following environment variables:

| Variable | Description | Default |
|---|---|---|
| `PORT` | The port the service will listen on. | `8080` |
| `AUDIT_RATE` | The deterministic sampling rate (0.0 to 1.0) for auditing. 1.0 means all messages are kept. | `1.0` |
| `DEFAULT_PHONE_REGION` | The default two-letter country code for E.164 phone number parsing. | `US` |
