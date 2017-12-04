# Prometheus Job Event Listener for Apache Oozie 

Exposes Apache Oozie job metrics to Prometheus.


## Metrics exposed

| Name                                  | Labels          | Description |
|---------------------------------------|-----------------|-------------|
| oozie_workflow_job_duration_seconds   | job_type, app_name, status | Duration of completed (Ooze status SUCCEEDED, KILLED, FAILED,...) job in seconds|
| oozie_workflow_job_state_time_seconds | job_type, app_name, status | Timestamp of completed job state change, including job name and changed state. Can be used for monitoring last successful run.
| oozie_workflow_job_total              | job_type, app_name, status | Count of completed jobs.

Example output:
```
# HELP oozie_workflow_job_state_time_seconds Timestamp of completed job state changes
# TYPE oozie_workflow_job_state_time_seconds gauge
oozie_workflow_job_state_time_seconds{job_type="WORKFLOW_JOB",app_name="test-3",status="SUSPENDED",} 1.512419363921E9
oozie_workflow_job_state_time_seconds{job_type="WORKFLOW_JOB",app_name="test-3",status="KILLED",} 1.512419353909E9
oozie_workflow_job_state_time_seconds{job_type="WORKFLOW_JOB",app_name="test-4",status="SUCCEEDED",} 1.51241937396E9
oozie_workflow_job_state_time_seconds{job_type="WORKFLOW_JOB",app_name="test-4",status="RUNNING",} 1.51241937396E9
oozie_workflow_job_state_time_seconds{job_type="WORKFLOW_JOB",app_name="test-3",status="RUNNING",} 1.512419363921E9
# HELP oozie_workflow_job_total Count of completed job state changes
# TYPE oozie_workflow_job_total counter
oozie_workflow_job_total{job_type="WORKFLOW_JOB",app_name="test-3",status="SUSPENDED",} 1.0
oozie_workflow_job_total{job_type="WORKFLOW_JOB",app_name="test-3",status="KILLED",} 2.0
oozie_workflow_job_total{job_type="WORKFLOW_JOB",app_name="test-4",status="SUCCEEDED",} 2.0
oozie_workflow_job_total{job_type="WORKFLOW_JOB",app_name="test-4",status="RUNNING",} 2.0
oozie_workflow_job_total{job_type="WORKFLOW_JOB",app_name="test-3",status="RUNNING",} 3.0
# HELP oozie_workflow_job_duration_seconds Duration of completed jobs
# TYPE oozie_workflow_job_duration_seconds gauge
oozie_workflow_job_duration_seconds{job_type="WORKFLOW_JOB",app_name="test-3",status="KILLED",} 667.0
oozie_workflow_job_duration_seconds{job_type="WORKFLOW_JOB",app_name="test-4",status="SUCCEEDED",} 0.0
```

## Installing

1) Add JAR with Prometheus Job Event Listener to Oozie WAR
   The shaded JAR already contains all required Prometheus Maven dependencies.
   ```
   cp oozie-prometheus-job-event-listener-VERSION-shaded.jar <OOZIE_SERVER_DIR>/oozie-server/webapps/oozie/WEB-INF/lib
   ```
   Alternatively, add the shaded JAR to the oozie.war and redeploy it.
   
2) Expose Prometheus metrics via simpleclient_servlet from Oozie web application

   Edit <OOZIE_SERVER_DIR>/oozie-server/webapps/oozie/WEB-INF/web.xml and add the Prometheus MetricServlet:
   ```
   <servlet>
        <servlet-name>Prometheus Metrics</servlet-name>
        <servlet-class>io.prometheus.client.exporter.MetricsServlet</servlet-class>
    </servlet>
    <servlet-mapping>
        <servlet-name>Prometheus Metrics</servlet-name>
        <url-pattern>/metrics</url-pattern>
    </servlet-mapping>
   ```
   The metrics will be available at http://<OOZIE-SERVER>:11000/oozie/metrics .

3) Add Oozie to Prometheus scraping
   Edit your Prometheus config and add the scrape config:
   ```
   - job_name: 'oozie_events'
     metrics_path: 'oozie/metrics'
     static_configs:
       - targets: ['localhost:11000']
   ```
   Note: Replace localhost with the name of your Oozie server, and change the Oozie default port if required.
   
## Building
```
mvn install
```
