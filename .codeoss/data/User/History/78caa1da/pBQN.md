This document outlines the key metrics and alerts configured in Cloud Monitoring for the DriveHealth ETL service.

Dashboard Widgets
The DriveHealth ETL Status dashboard provides a real-time view of the pipeline's health. It is comprised of the following widgets:

Cloud Run: requests by code: Tracks the number of 2xx, 4xx, and 5xx requests to the /pubsub endpoint. A high rate of 4xx errors indicates terminal validation failures, while a spike in 5xx suggests transient errors that require a retry.

Cloud Run: p50/p95 latency: Monitors the 50th and 95th percentile latency of the service, ensuring that performance remains stable under load, ideally with p95 latency staying below 1 second.

Cloud Run: instance count: Shows the number of Cloud Run instances actively running. This is a key metric for observing the service's autoscaling behavior in response to varying message rates.

Pub/Sub: DLQ backlog: Displays the number of undelivered messages in the Dead Letter Queue. A persistent backlog indicates a problem with terminal messages that are not being re-ingested or require manual intervention.

BigQuery: write errors/latency: Tracks any errors that occur during streaming inserts into the BigQuery table. This metric is a log-based metric derived from the structured logs within the service.

Log-based metric: observed sampling rate: A custom metric that verifies the AUDIT_RATE by comparing the number of sampled events to the total number of received events.

Alerting Policies
The following alerts are enabled to ensure timely notification of critical issues:


No Successful BigQuery Writes: This alert triggers if the service fails to write successfully to BigQuery for a period of 5 minutes.


DLQ Backlog has messages: This alert is configured to fire if the DLQ backlog exceeds a predefined threshold for 10 minutes, indicating a potential issue with failed messages.


Cloud Run 5xx Error Rate Spike: This alert activates when the rate of 5xx errors from the Cloud Run service spikes for a period of 5 minutes, signaling a systemic issue that is causing repeated transient failures.