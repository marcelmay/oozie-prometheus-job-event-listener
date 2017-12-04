package de.m3y.oozie.prometheus;

import io.prometheus.client.Gauge;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.event.JobEvent;
import org.apache.oozie.event.*;
import org.apache.oozie.event.listener.JobEventListener;
import org.apache.oozie.util.XLog;

/**
 * Provides Oozie job metrics for Prometheus:
 * <ul>
 *     <li>oozie_workflow_job_duration_seconds : duration of job.</li>
 *     <li>oozie_workflow_job_state_time_seconds : timestamp of job state change, including job name and changed state. Can be used for monitoring last successful run.</li>
 * </ul>
 */
public class PrometheusJobEventListener extends JobEventListener {
    private XLog LOG = XLog.getLog(PrometheusJobEventListener.class);

    private static final Gauge WORKFLOW_DURATION = Gauge.build()
            .name("oozie_workflow_job_duration_seconds")
            .help("oozie_workflow_job_duration_seconds")
            .labelNames("job_type", "app_name", "status")
            .register();
    private static final Gauge WORKFLOW_EVENT = Gauge.build()
            .name("oozie_workflow_job_state_time_seconds")
            .help("oozie_workflow_job_state_time_seconds")
            .labelNames("job_type", "app_name", "status")
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

        updateDuration(workflowJobEvent, workflowJobEvent.getStatus().name());
        updateStatusEvent(workflowJobEvent, workflowJobEvent.getStatus().name());
    }


    @Override
    public void onWorkflowActionEvent(WorkflowActionEvent workflowActionEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(workflowActionEvent);
        }

        updateDuration(workflowActionEvent, workflowActionEvent.getStatus().name());
        updateStatusEvent(workflowActionEvent, workflowActionEvent.getStatus().name());
    }

    @Override
    public void onCoordinatorJobEvent(CoordinatorJobEvent coordinatorJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(coordinatorJobEvent);
        }

        updateDuration(coordinatorJobEvent, coordinatorJobEvent.getStatus().name());
        updateStatusEvent(coordinatorJobEvent, coordinatorJobEvent.getStatus().name());
    }

    @Override
    public void onCoordinatorActionEvent(CoordinatorActionEvent coordinatorActionEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(coordinatorActionEvent);
        }

        updateDuration(coordinatorActionEvent, coordinatorActionEvent.getStatus().name());
        updateStatusEvent(coordinatorActionEvent, coordinatorActionEvent.getStatus().name());
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
        WORKFLOW_EVENT.labels(
                event.getAppType().name(),
                event.getAppName(),
                status
        ).setToCurrentTime();
    }

    private void updateDuration(JobEvent jobEvent, String status) {
        if (null != jobEvent.getEndTime() && null != jobEvent.getStartTime()) {
            long time = jobEvent.getEndTime().getTime() - jobEvent.getStartTime().getTime();
            WORKFLOW_DURATION.labels(
                    jobEvent.getAppType().name(),
                    jobEvent.getAppName(),
                    status
            ).set(time/1000L /* Convert ms to s */);
        }
    }
}
