package de.m3y.oozie.prometheus;


import java.util.Date;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.CollectorRegistry;
import org.apache.oozie.client.Job;
import org.apache.oozie.event.BundleJobEvent;
import org.junit.Test;

import static de.m3y.prometheus.assertj.MetricFamilySamplesAssert.assertThat;
import static de.m3y.prometheus.assertj.MetricFamilySamplesAssert.labelValues;
import static de.m3y.prometheus.assertj.MetricFamilySamplesUtils.getMetricFamilySamples;
import static org.assertj.core.api.Assertions.within;

public class PrometheusJobEventListenerTest {
    @Test
    public void testOnBundleJobEvent() {
        CollectorRegistry.defaultRegistry.clear();

        PrometheusJobEventListener prometheusJobEventListener = new PrometheusJobEventListener();

        long start0 = System.currentTimeMillis();
        long end0 = start0 + 100L;
        final BundleJobEvent jobEvent = new BundleJobEvent("id#0", Job.Status.PREP, "user #0", "app #0",
                new Date(start0), new Date(end0));
        prometheusJobEventListener.onBundleJobEvent(jobEvent);

        MetricFamilySamples mfsWorkflowJobTotal = getMetricFamilySamples("oozie_workflow_job_total");
        assertThat(mfsWorkflowJobTotal)
                .hasTypeOfCounter()
                .hasSampleLabelNames("job_type", "app_name", "status")
                .hasSampleValue(
                        labelValues(jobEvent.getAppType().name(), jobEvent.getAppName(), jobEvent.getStatus().name()),
                        1d /* one count */
                );

        MetricFamilySamples mfsWorkflowJobDuration = getMetricFamilySamples("oozie_workflow_job_duration_seconds");
        assertThat(mfsWorkflowJobDuration)
                .hasTypeOfGauge()
                .hasSampleLabelNames("job_type", "app_name", "status")
                .hasSampleValue(
                        labelValues(jobEvent.getAppType().name(), jobEvent.getAppName(), jobEvent.getStatus().name()),
                        (end0 - start0) / 1000d // ms to seconds
                );

        MetricFamilySamples mfsWorkflowJobStateTime = getMetricFamilySamples("oozie_workflow_job_state_time_seconds");
        assertThat(mfsWorkflowJobStateTime)
                .hasTypeOfGauge()
                .hasAnySamples()
                .hasSampleLabelNames("job_type", "app_name", "status")
                .hasSampleValue(
                        labelValues(jobEvent.getAppType().name(), jobEvent.getAppName(), jobEvent.getStatus().name()),
                        da -> da.isEqualTo(start0 / 1000d, within(1d))
                );
    }
}
