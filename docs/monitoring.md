Of course. Here is the updated `monitoring.md` file, now including a detailed, step-by-step guide for creating the custom log-based metric to observe the sampling rate, as requested by the review.

-----

### **Updated `monitoring.md`**

This document outlines the key metrics and alerts configured in Cloud Monitoring for the DriveHealth ETL service. These components provide a real-time, comprehensive view of the pipeline's health, performance, and reliability.

#### **Dashboard: DriveHealth ETL Status**

A provisioned dashboard provides an at-a-glance view of the entire pipeline. It is comprised of the following critical widgets:

  * **Cloud Run: Request Count**: Tracks the rate of incoming requests, broken down by HTTP status code (`2xx`, `4xx`, `5xx`). A high rate of `2xx` codes indicates healthy processing. A spike in `4xx` errors points to terminal validation failures, while a spike in `5xx` suggests transient system errors that should trigger retries.
  * **Cloud Run: p95 Latency**: Monitors the 95th percentile latency of the service. This is a key indicator of user-facing performance. The goal is to keep this value low (ideally under 1 second) even during high traffic, proving the service is efficient.
  * **Cloud Run: Instance Count**: Shows the number of Cloud Run instances actively running. This is the primary metric for observing the service's autoscaling behavior in response to varying message rates.
  * **Pub/Sub: DLQ Backlog**: Displays the number of undelivered messages in the Dead Letter Queue subscription (`call-audits-dlq-sub`). A rising or persistent backlog indicates an issue with terminal message failures that may require manual intervention or a code fix.
  * **Log-based Metric: BigQuery Streaming Insert Errors**: A custom metric that tracks any errors occurring during the final write to the BigQuery table. "No data" on this chart is the ideal state.
  * **Log-based Metric: Observed Sampling Rate**: A custom ratio chart that verifies the configured `AUDIT_RATE`. It is created by dividing the count of sampled events by the total number of received events over a time window.

#### **Alerting Policies**

The following alerts are enabled to ensure the operations team is notified of critical issues before they impact users:

  * **High 5xx Error Rate**: Triggers if the rate of `5xx` server errors from the Cloud Run service exceeds a defined threshold for 5 minutes.
  * **DLQ Backlog Growing**: Triggers if the number of messages in the DLQ backlog exceeds a threshold for more than 10 minutes.
  * **No Successful BigQuery Writes**: A critical alert that triggers if the service fails to log a successful BigQuery write for more than 5 minutes.

-----

### **Creating Log-Based Metrics**

The structured JSON logs produced by this service are designed to be easily converted into powerful metrics for dashboards and alerts.

#### **Creating the Observed Sampling Rate Metric**

To monitor the effective sampling rate, you need to create two counter metrics and then combine them in a chart using MQL (Monitoring Query Language).

**Step 1: Create the "Sampled Events" Counter**

1.  Navigate to **Logs Explorer** in the Google Cloud Console.
2.  In the query box, filter for logs that represent a sampled event. The log message and the `sampled` field are both reliable filters.
    ```
    resource.type="cloud_run_revision"
    resource.labels.service_name="your-etl-service"
    jsonPayload.message="Event sampled for processing"
    jsonPayload.sampled=true
    ```
3.  Click the **"Create metric"** button above the query results.
4.  Set the following properties:
      * **Metric Type**: Counter
      * **Log metric name**: `etl_events_sampled_count`
      * **Description**: "Counts the number of ETL events that were sampled for processing."
      * **Units**: `1`
5.  Click **"Create Metric"**.

**Step 2: Create the "Total Received Events" Counter**

1.  In the Logs Explorer, update the filter to count *all* sampling decisions (both sampled in and sampled out).
    ```
    resource.type="cloud_run_revision"
    resource.labels.service_name="your-etl-service"
    (jsonPayload.message="Event sampled for processing" OR jsonPayload.message="Event not sampled")
    ```
2.  Click **"Create metric"** again.
3.  Set the following properties:
      * **Metric Type**: Counter
      * **Log metric name**: `etl_events_total_count`
      * **Description**: "Counts the total number of ETL events that underwent a sampling decision."
      * **Units**: `1`
4.  Click **"Create Metric"**.

**Step 3: Create the Ratio Chart in the Dashboard**

1.  Navigate to your Monitoring Dashboard and add a new widget.
2.  Select the **"Line Chart"** visualization.
3.  Switch to the **"MQL"** query editor tab.
4.  Enter the following query to calculate the ratio:
    ```mql
    fetch cloud_run_revision
    | {
        metric 'logging.googleapis.com/user/etl_events_sampled_count';
        metric 'logging.googleapis.com/user/etl_events_total_count'
      }
    | ratio
    ```
5.  Give the chart a title like "Observed Sampling Rate" and save it. This widget will now show the actual sampling percentage over time.