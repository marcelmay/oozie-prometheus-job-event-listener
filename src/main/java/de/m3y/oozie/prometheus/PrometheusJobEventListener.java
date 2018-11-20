package de.m3y.oozie.prometheus;

import java.util.Date;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.event.*;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.util.XLog;

/**
 * Provides Oozie job metrics for Prometheus:
 * <ul>
 * <li>oozie_workflow_job_duration_seconds : Duration of completed jobs, including job name and changed state. Can be used for alerting on lame jobs.</li>
 * <li>oozie_workflow_job_state_time_seconds : Timestamp of completed job state changes, including job name and changed state. Can be used for alerting on last successful run</li>
 * <li>oozie_workflow_job_total : Count of completed job state changes, including job name and changed state. Can be used to trigger alert on job failure rate.</li>
 * </ul>
 */
public class PrometheusJobEventListener extends JobEventListener {
    private XLog LOG = XLog.getLog(PrometheusJobEventListener.class);

    private static final String LABEL_JOB_TYPE = "job_type";
    private static final String LABEL_APP_NAME = "app_name";
    private static final String LABEL_STATUS = "status";

    private static final Gauge WORKFLOW_JOB_DURATION = Gauge.build()
            .name("oozie_workflow_job_duration_seconds")
            .help("Duration of completed jobs")
            .labelNames(LABEL_JOB_TYPE, LABEL_APP_NAME, LABEL_STATUS)
            .register();
    private static final Gauge WORKFLOW_JOB_EVENT = Gauge.build()
            .name("oozie_workflow_job_state_time_seconds")
            .help("Timestamp of completed job state changes")
            .labelNames(LABEL_JOB_TYPE, LABEL_APP_NAME, LABEL_STATUS)
            .register();
    private static final Counter WORKFLOW_JOB_EVENT_TOTAL = Counter.build()
            .name("oozie_workflow_job_total")
            .help("Count of completed job state changes")
            .labelNames(LABEL_JOB_TYPE, LABEL_APP_NAME, LABEL_STATUS)
            .register();

    @Override
    public void init(Configuration configuration) {
        LOG.info("Initializing Prometheus Job Event Listener");
    }

    @Override
    public void destroy() {
        LOG.info("Destroying Prometheus Job Event Listener");
    }

    @Override
    public void onWorkflowJobEvent(WorkflowJobEvent workflowJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(workflowJobEvent);
        }

        final String statusName = workflowJobEvent.getStatus().name();
        updateDuration(workflowJobEvent, statusName);
        updateStatusEvent(workflowJobEvent, statusName);
    }


    @Override
    public void onWorkflowActionEvent(WorkflowActionEvent workflowActionEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(workflowActionEvent);
        }

        final String statusName = workflowActionEvent.getStatus().name();
        updateDuration(workflowActionEvent, statusName);
        updateStatusEvent(workflowActionEvent, statusName);
    }

    @Override
    public void onCoordinatorJobEvent(CoordinatorJobEvent coordinatorJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(coordinatorJobEvent);
        }

        final String statusName = coordinatorJobEvent.getStatus().name();
        updateDuration(coordinatorJobEvent, statusName);
        updateStatusEvent(coordinatorJobEvent, statusName);
    }

    @Override
    public void onCoordinatorActionEvent(CoordinatorActionEvent coordinatorActionEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(coordinatorActionEvent);
        }

        final String statusName = coordinatorActionEvent.getStatus().name();
        updateDuration(coordinatorActionEvent, statusName);
        updateStatusEvent(coordinatorActionEvent, statusName);
    }

    @Override
    public void onBundleJobEvent(BundleJobEvent bundleJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(bundleJobEvent);
        }

        updateDuration(bundleJobEvent, bundleJobEvent.getStatus().name());
        updateStatusEvent(bundleJobEvent, bundleJobEvent.getStatus().name());
    }

    private void updateStatusEvent(JobEvent event, String status) {
        final String appName = event.getAppType().name();
        WORKFLOW_JOB_EVENT.labels(
                appName,
                event.getAppName(),
                status
        ).setToCurrentTime();
        WORKFLOW_JOB_EVENT_TOTAL.labels(
                appName,
                event.getAppName(),
                status
        ).inc();
    }

    private void updateDuration(JobEvent jobEvent, String status) {
        final Date endTime = jobEvent.getEndTime();
        final Date startTime = jobEvent.getStartTime();
        if (null != endTime && null != startTime) {
            final long duration = endTime.getTime() - startTime.getTime();
            WORKFLOW_JOB_DURATION.labels(
                    jobEvent.getAppType().name(),
                    jobEvent.getAppName(),
                    status
            ).set(duration / 1000d /* Convert ms to seconds */);
        }
    }
}
