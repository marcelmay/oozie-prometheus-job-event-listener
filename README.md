# Prometheus Job Event Listener for Apache Oozie 

Exposes Apache Oozie job metrics to Prometheus.


## Metrics exposed

| Name                                  | Labels          | Description |
|---------------------------------------|-----------------|-------------|
| oozie_workflow_job_duration_seconds   | job_type, app_name, status| Duration of finished (Ooze status SUCCEEDED, KILLED, FAILED,...) job in seconds|
| oozie_workflow_job_state_time_seconds | job_type, app_name, status | Timestamp of job state change, including job name and changed state. Can be used for monitoring last successful run.

Example output:
```
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

3) Add Oozie to Prometheus scraping
   Edit your Prometheus config and add the scrape config:
   ```
   - job_name: 'oozie-events'
     metrics_path: 'oozie/metrics'
     static_configs:
       - targets: ['localhost:11000']
   ```
   Note: Replace localhost with the name of your Oozie server, and change the Oozie default port if required.
   
## Building
```
mvn install
```
