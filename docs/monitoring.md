This document outlines the key metrics and alerts configured in Cloud Monitoring for the DriveHealth ETL service. These components provide a real-time, comprehensive view of the pipeline's health, performance, and reliability.

Dashboard: DriveHealth ETL Status
A provisioned dashboard provides an at-a-glance view of the entire pipeline. It is comprised of the following critical widgets:

Cloud Run: Request Count: Tracks the rate of incoming requests, broken down by HTTP status code (2xx, 4xx, 5xx). A high rate of 2xx codes indicates healthy processing. A spike in 4xx errors points to terminal validation failures, while a spike in 5xx suggests transient system errors that should trigger retries.

Cloud Run: p95 Latency: Monitors the 95th percentile latency of the service. This is a key indicator of user-facing performance. The goal is to keep this value low (ideally under 1 second) even during high traffic, proving the service is efficient.

Cloud Run: Instance Count: Shows the number of Cloud Run instances actively running. This is the primary metric for observing the service's autoscaling behavior in response to varying message rates.

Pub/Sub: DLQ Backlog: Displays the number of undelivered messages in the Dead Letter Queue subscription (call-audits-dlq-sub). A rising or persistent backlog indicates an issue with terminal message failures that may require manual intervention or a code fix.

Log-based Metric: BigQuery Streaming Insert Errors: A custom metric that tracks any errors occurring during the final write to the BigQuery table. "No data" on this chart is the ideal state.

Log-based Metric: Observed Sampling Rate: A custom metric that verifies the configured AUDIT_RATE. It is created by dividing the count of logs where sampled: true by the total number of ingestion logs over a time window.

Alerting Policies
The following alerts are enabled to ensure the operations team is notified of critical issues before they impact users:

High 5xx Error Rate: Triggers if the rate of 5xx server errors from the Cloud Run service exceeds a defined threshold for 5 minutes. This indicates a systemic issue causing repeated transient failures.

DLQ Backlog Growing: Triggers if the number of messages in the DLQ backlog exceeds a threshold for more than 10 minutes, indicating a potential issue with a bad deployment or a problematic data source.

No Successful BigQuery Writes: A critical alert that triggers if the service fails to log a successful BigQuery write for more than 5 minutes, suggesting a complete pipeline stall.