package de.m3y.oozie.prometheus;


import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import org.apache.oozie.client.Job;
import org.apache.oozie.event.BundleJobEvent;
import org.junit.Test;

import static de.m3y.oozie.prometheus.MetricFamilySamplesAssert.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;

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

        final List<Collector.MetricFamilySamples> list = Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples());
        for (Collector.MetricFamilySamples metricFamilySamples : list) {
            final List<Collector.MetricFamilySamples.Sample> samples = metricFamilySamples.samples;
            switch (metricFamilySamples.name) {
                case "oozie_workflow_job_total":
                    assertThat(metricFamilySamples)
                            .hasType(Collector.Type.COUNTER)
                            .hasSample(
                                    Arrays.asList("job_type", "app_name", "status"),
                                    Arrays.asList(jobEvent.getAppType().name(), jobEvent.getAppName(), jobEvent.getStatus().name()),
                                    1d /* one count */
                            );
                    break;
                case "oozie_workflow_job_duration_seconds":
                    assertThat(metricFamilySamples)
                            .hasType(Collector.Type.GAUGE)
                            .hasSample(
                                    Arrays.asList("job_type", "app_name", "status"),
                                    Arrays.asList(jobEvent.getAppType().name(), jobEvent.getAppName(), jobEvent.getStatus().name()),
                                    (double) (end0 - start0) / 1000d /* ms to seconds */
                            );
                    break;
                case "oozie_workflow_job_state_time_seconds":
                    assertThat(metricFamilySamples)
                            .hasType(Collector.Type.GAUGE)
                            .hasAnySamples()
                            .hasSample(
                                    Arrays.asList("job_type", "app_name", "status")
                            )
                            .hasSample(
                                    Arrays.asList("job_type", "app_name", "status"),
                                    Arrays.asList(jobEvent.getAppType().name(), jobEvent.getAppName(), jobEvent.getStatus().name())
                            )
                            .hasSample(
                                    Arrays.asList("job_type", "app_name", "status"),
                                    Arrays.asList(jobEvent.getAppType().name(), jobEvent.getAppName(), jobEvent.getStatus().name()),
                                    start0 / 1000d, 1d /* error tolerance of 1s */
                            );
                    break;
                default:
                    fail("Unexpected metric name " + metricFamilySamples.name);
            }
        }

    }
}
