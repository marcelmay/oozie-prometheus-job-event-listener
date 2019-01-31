package de.m3y.oozie.prometheus;

import java.util.Date;

import io.prometheus.client.CollectorRegistry;
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
 * <li>oozie_workflow_job_duration_seconds : Duration of completed jobs, including job name and changed state.
 * Can be used for alerting on lame jobs.</li>
 * <li>oozie_workflow_job_state_time_seconds : Timestamp of completed job state changes, including job name and
 * changed state. Can be used for alerting on last successful run</li>
 * <li>oozie_workflow_job_total : Count of completed job state changes, including job name and changed state.
 * Can be used to trigger alert on job failure rate.</li>
 * </ul>
 */
public class PrometheusJobEventListener extends JobEventListener {
    private XLog LOG = XLog.getLog(PrometheusJobEventListener.class);

    private static final String LABEL_JOB_TYPE = "job_type";
    private static final String LABEL_APP_NAME = "app_name";
    private static final String LABEL_STATUS = "status";

    private final Gauge workflowJobDuration = Gauge.build()
            .name("oozie_workflow_job_duration_seconds")
            .help("Duration of completed jobs")
            .labelNames(LABEL_JOB_TYPE, LABEL_APP_NAME, LABEL_STATUS)
            .create();
    private final Gauge workflowJobEventTimestamp = Gauge.build()
            .name("oozie_workflow_job_state_time_seconds")
            .help("Timestamp of completed job state changes")
            .labelNames(LABEL_JOB_TYPE, LABEL_APP_NAME, LABEL_STATUS)
            .create();
    private final Counter workflowJobTotal = Counter.build()
            .name("oozie_workflow_job_total")
            .help("Count of completed job state changes")
            .labelNames(LABEL_JOB_TYPE, LABEL_APP_NAME, LABEL_STATUS)
            .create();

    @Override
    public void init(Configuration configuration) {
        LOG.info("Initializing Prometheus Job Event Listener");
        workflowJobDuration.register();
        workflowJobEventTimestamp.register();
        workflowJobTotal.register();
    }

    @Override
    public void destroy() {
        LOG.info("Destroying Prometheus Job Event Listener");
        CollectorRegistry.defaultRegistry.unregister(workflowJobDuration);
        CollectorRegistry.defaultRegistry.unregister(workflowJobEventTimestamp);
        CollectorRegistry.defaultRegistry.unregister(workflowJobTotal);
    }

    @Override
    public void onWorkflowJobEvent(WorkflowJobEvent workflowJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received workflow job event {}", workflowJobEvent);
        }

        final String statusName = workflowJobEvent.getStatus().name();
        final String[] labels = getLabels(workflowJobEvent, statusName);
        updateDuration(workflowJobEvent, labels);
        updateStatusEvent(labels);
    }


    @Override
    public void onWorkflowActionEvent(WorkflowActionEvent workflowActionEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received workflow action event {}", workflowActionEvent);
        }

        final String statusName = workflowActionEvent.getStatus().name();
        final String[] labels = getLabels(workflowActionEvent, statusName);
        updateDuration(workflowActionEvent, labels);
        updateStatusEvent(labels);
    }

    @Override
    public void onCoordinatorJobEvent(CoordinatorJobEvent coordinatorJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received coordinator job event {}", coordinatorJobEvent);
        }

        final String statusName = coordinatorJobEvent.getStatus().name();
        final String[] labels = getLabels(coordinatorJobEvent, statusName);
        updateDuration(coordinatorJobEvent, labels);
        updateStatusEvent(labels);
    }

    @Override
    public void onCoordinatorActionEvent(CoordinatorActionEvent coordinatorActionEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received coordinator action event {}", coordinatorActionEvent);
        }

        final String statusName = coordinatorActionEvent.getStatus().name();
        final String[] labels = getLabels(coordinatorActionEvent, statusName);
        updateDuration(coordinatorActionEvent, labels);
        updateStatusEvent(labels);
    }

    @Override
    public void onBundleJobEvent(BundleJobEvent bundleJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received bundle job event {}", bundleJobEvent);
        }

        final String statusName = bundleJobEvent.getStatus().name();
        final String[] labels = getLabels(bundleJobEvent, statusName);
        updateDuration(bundleJobEvent, labels);
        updateStatusEvent(labels);
    }

    private String[] getLabels(JobEvent jobEvent, String status) {
        return new String[]{jobEvent.getAppType().name(), jobEvent.getAppName(), status};
    }

    private void updateStatusEvent(String[] labels) {
        workflowJobEventTimestamp.labels(labels)
                .setToCurrentTime();
        workflowJobTotal.labels(labels)
                .inc();
    }

    private void updateDuration(JobEvent jobEvent, String[] labels) {
        final Date endTime = jobEvent.getEndTime();
        final Date startTime = jobEvent.getStartTime();
        if (null != endTime && null != startTime) {
            final long duration = endTime.getTime() - startTime.getTime();
            workflowJobDuration.labels(labels)
                    .set(duration / 1000d /* Convert ms to seconds */);
        }
    }
}
