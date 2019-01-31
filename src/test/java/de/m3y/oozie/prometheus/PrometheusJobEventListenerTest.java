package de.m3y.oozie.prometheus;


import java.util.Date;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.CollectorRegistry;
import org.apache.oozie.AppType;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.event.*;
import org.junit.Before;
import org.junit.Test;

import static de.m3y.prometheus.assertj.MetricFamilySamplesAssert.assertThat;
import static de.m3y.prometheus.assertj.MetricFamilySamplesAssert.labelValues;
import static de.m3y.prometheus.assertj.MetricFamilySamplesUtils.getMetricFamilySamples;
import static org.assertj.core.api.Assertions.within;

public class PrometheusJobEventListenerTest {
    @Before
    public void setUp() {
        CollectorRegistry.defaultRegistry.clear();
    }

    @Test
    public void testOnWorkflowJobEvent() {
        PrometheusJobEventListener prometheusJobEventListener = createAndInitPrometheusJobEventListener();

        long start0 = System.currentTimeMillis();
        long end0 = start0 + 100L;
        final WorkflowJobEvent jobEvent = new WorkflowJobEvent("id#0", "parentId",
                WorkflowJob.Status.SUCCEEDED, "user #0", "app #0",
                new Date(start0), new Date(end0));
        prometheusJobEventListener.onWorkflowJobEvent(jobEvent);

        validateJobEvent(start0, end0, jobEvent.getAppType(), jobEvent.getAppName(), jobEvent.getStatus().name());
    }

    @Test
    public void testOnWorkflowActionEvent() {
        PrometheusJobEventListener prometheusJobEventListener = createAndInitPrometheusJobEventListener();

        long start0 = System.currentTimeMillis();
        long end0 = start0 + 100L;
        final WorkflowActionEvent jobEvent = new WorkflowActionEvent("id#0", "parentId",
                WorkflowAction.Status.START_RETRY, "user #0", "app #0",
                new Date(start0), new Date(end0));
        prometheusJobEventListener.onWorkflowActionEvent(jobEvent);

        validateJobEvent(start0, end0, jobEvent.getAppType(), jobEvent.getAppName(), jobEvent.getStatus().name());
    }

    @Test
    public void testOnCoordinatorJobEvent() {
        PrometheusJobEventListener prometheusJobEventListener = createAndInitPrometheusJobEventListener();

        long start0 = System.currentTimeMillis();
        long end0 = start0 + 100L;
        final CoordinatorJobEvent jobEvent = new CoordinatorJobEvent("id#0", "parentId",
                Job.Status.FAILED, "user #0", "app #0",
                new Date(start0), new Date(end0));
        prometheusJobEventListener.onCoordinatorJobEvent(jobEvent);

        validateJobEvent(start0, end0, jobEvent.getAppType(), jobEvent.getAppName(), jobEvent.getStatus().name());
    }

    @Test
    public void testOnCoordinatorActionEvent() {
        PrometheusJobEventListener prometheusJobEventListener = createAndInitPrometheusJobEventListener();

        long start0 = System.currentTimeMillis();
        long end0 = start0 + 100L;
        final CoordinatorActionEvent jobEvent = new CoordinatorActionEvent("id#0", "parentId",
                CoordinatorAction.Status.READY, "user #0", "app #0",
                null, new Date(start0),
                "missing deps");
        jobEvent.setEndTime(new Date(end0));
        prometheusJobEventListener.onCoordinatorActionEvent(jobEvent);

        validateJobEvent(start0, end0, jobEvent.getAppType(), jobEvent.getAppName(), jobEvent.getStatus().name());
    }

    @Test
    public void testOnBundleJobEvent() {
        PrometheusJobEventListener prometheusJobEventListener = createAndInitPrometheusJobEventListener();

        long start0 = System.currentTimeMillis();
        long end0 = start0 + 100L;
        final BundleJobEvent jobEvent = new BundleJobEvent("id#0", Job.Status.PREP, "user #0", "app #0",
                new Date(start0), new Date(end0));
        prometheusJobEventListener.onBundleJobEvent(jobEvent);

        validateJobEvent(start0, end0, jobEvent.getAppType(), jobEvent.getAppName(), jobEvent.getStatus().name());
    }

    @Test
    public void testInitAndDestroy() {
        for(int i=0;i<2;i++) {
            PrometheusJobEventListener prometheusJobEventListener = createAndInitPrometheusJobEventListener();

            long start0 = System.currentTimeMillis();
            long end0 = start0 + 100L;
            final BundleJobEvent jobEvent = new BundleJobEvent("id#0", Job.Status.PREP, "user #0", "app #0",
                    new Date(start0), new Date(end0));
            prometheusJobEventListener.onBundleJobEvent(jobEvent);

            validateJobEvent(start0, end0, jobEvent.getAppType(), jobEvent.getAppName(), jobEvent.getStatus().name());

            prometheusJobEventListener.destroy();
        }
    }

    private PrometheusJobEventListener createAndInitPrometheusJobEventListener() {
        PrometheusJobEventListener prometheusJobEventListener = new PrometheusJobEventListener();
        prometheusJobEventListener.init(null);
        return prometheusJobEventListener;
    }

    private void validateJobEvent(long start0, long end0, AppType appType, String appName, String name) {
        MetricFamilySamples mfsWorkflowJobTotal = getMetricFamilySamples("oozie_workflow_job_total");
        assertThat(mfsWorkflowJobTotal)
                .hasTypeOfCounter()
                .hasSampleLabelNames("job_type", "app_name", "status")
                .hasSampleValue(
                        labelValues(appType.name(), appName, name),
                        1d /* one count */
                );

        MetricFamilySamples mfsWorkflowJobDuration = getMetricFamilySamples("oozie_workflow_job_duration_seconds");
        assertThat(mfsWorkflowJobDuration)
                .hasTypeOfGauge()
                .hasSampleLabelNames("job_type", "app_name", "status")
                .hasSampleValue(
                        labelValues(appType.name(), appName, name),
                        (end0 - start0) / 1000d // ms to seconds
                );

        MetricFamilySamples mfsWorkflowJobStateTime = getMetricFamilySamples("oozie_workflow_job_state_time_seconds");
        assertThat(mfsWorkflowJobStateTime)
                .hasTypeOfGauge()
                .hasAnySamples()
                .hasSampleLabelNames("job_type", "app_name", "status")
                .hasSampleValue(
                        labelValues(appType.name(), appName, name),
                        da -> da.isEqualTo(start0 / 1000d, within(1d))
                );
    }
}
