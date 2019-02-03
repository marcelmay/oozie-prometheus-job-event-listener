package de.m3y.oozie.prometheus;


import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.CollectorRegistry;
import org.apache.oozie.AppType;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.event.*;
import org.assertj.core.api.Assertions;
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
        for (int i = 0; i < 2; i++) {
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

    @Test
    public void testPurgeTtlConfiguration() {
        PrometheusJobEventListener prometheusJobEventListener = createAndInitPrometheusJobEventListener();
        Assertions.assertThat(prometheusJobEventListener.timeSeriesIdleTtlMs)
                .isEqualTo(TimeUnit.DAYS.toMillis(PrometheusJobEventListener.DEFAULT_TIME_SERIES_IDLE_TTL_DAYS));
        prometheusJobEventListener.destroy();

        System.setProperty(PrometheusJobEventListener.PROPERTY_TIME_SERIES_IDLE_TTL_SECONDS, "1");
        prometheusJobEventListener = createAndInitPrometheusJobEventListener();
        Assertions.assertThat(prometheusJobEventListener.timeSeriesIdleTtlMs)
                .isEqualTo(TimeUnit.SECONDS.toMillis(1));
        prometheusJobEventListener.destroy();
    }

    @Test
    public void testPurgeMultithreaded() throws InterruptedException {
        System.setProperty(PrometheusJobEventListener.PROPERTY_TIME_SERIES_IDLE_TTL_SECONDS, "1");
        System.setProperty(PrometheusJobEventListener.PROPERTY_PURGE_INTERVAL, "1");
        PrometheusJobEventListener prometheusJobEventListener = createAndInitPrometheusJobEventListener();

        AtomicInteger eventCounter = new AtomicInteger();
        RandomProducerThread p1 = new RandomProducerThread(prometheusJobEventListener, eventCounter);
        p1.start();
        RandomProducerThread p2 = new RandomProducerThread(prometheusJobEventListener, eventCounter);
        p2.start();
        RandomProducerThread p3 = new RandomProducerThread(prometheusJobEventListener, eventCounter);
        p3.start();

        // Wait for expected purge timeout
        await(this, 500L);
        MetricFamilySamples purgeDuration = getMetricFamilySamples(PrometheusJobEventListener.METRIC_PURGE_DURATION);
        assertThat(purgeDuration).hasTypeOfSummary().hasSampleCountValue(0);
        await(this, 550L);
        int s2 = eventCounter.get();

        purgeDuration = getMetricFamilySamples(PrometheusJobEventListener.METRIC_PURGE_DURATION);
        assertThat(purgeDuration).hasTypeOfSummary().hasSampleCountValue(1);

        await(this, 1050L);
        purgeDuration = getMetricFamilySamples(PrometheusJobEventListener.METRIC_PURGE_DURATION);
        assertThat(purgeDuration).hasTypeOfSummary().hasSampleCountValue(2);

        Assertions.assertThat(eventCounter.get()).isGreaterThan(100); // At least 100 created events

        // Stop producers
        p1.doStop();
        p2.doStop();
        p3.doStop();
        p1.join(100);
        p2.join(100);
        p3.join(100);

        // Wait for expected purge timeout
        await(this, 1010L);
        // Trigger purge indirectly by new sample
        prometheusJobEventListener.onBundleJobEvent(new BundleJobEvent("id#trigger", Job.Status.PREP, "user #0",
                "app trigger",
                new Date(0), new Date(10)));

        // Expect only trigger event
        MetricFamilySamples mfsWorkflowJobTotal = getMetricFamilySamples("oozie_workflow_job_total");
        assertThat(mfsWorkflowJobTotal).hasSampleSize(1);
        MetricFamilySamples mfsWorkflowJobDuration = getMetricFamilySamples("oozie_workflow_job_duration_seconds");
        assertThat(mfsWorkflowJobDuration).hasSampleSize(1);
        MetricFamilySamples mfsWorkflowJobStateTime = getMetricFamilySamples("oozie_workflow_job_state_time_seconds");
        assertThat(mfsWorkflowJobStateTime).hasSampleSize(1);

        prometheusJobEventListener.destroy();
    }

    @Test
    public void testPurge() {
        System.setProperty(PrometheusJobEventListener.PROPERTY_TIME_SERIES_IDLE_TTL_SECONDS, "1");
        System.setProperty(PrometheusJobEventListener.PROPERTY_PURGE_INTERVAL, "1");
        PrometheusJobEventListener prometheusJobEventListener = createAndInitPrometheusJobEventListener();

        // Create and expect events, distinguished by job_type/app_name/status
        int distinguishedJobEventCount = 10;
        for (int i = 0; i < distinguishedJobEventCount; i++) {
            long start0 = System.currentTimeMillis();
            long end0 = start0 + 100L;
            final BundleJobEvent jobEvent = new BundleJobEvent("id#" + i, Job.Status.PREP, "user #0",
                    "app #" + i /* distinguished app name */,
                    new Date(start0), new Date(end0));
            prometheusJobEventListener.onBundleJobEvent(jobEvent);
            validateJobEvent(start0, end0, jobEvent.getAppType(), jobEvent.getAppName(), jobEvent.getStatus().name());
        }

        MetricFamilySamples mfsWorkflowJobTotal = getMetricFamilySamples("oozie_workflow_job_total");
        assertThat(mfsWorkflowJobTotal).hasSampleSize(distinguishedJobEventCount);
        MetricFamilySamples mfsWorkflowJobDuration = getMetricFamilySamples("oozie_workflow_job_duration_seconds");
        assertThat(mfsWorkflowJobDuration).hasSampleSize(distinguishedJobEventCount);
        MetricFamilySamples mfsWorkflowJobStateTime = getMetricFamilySamples("oozie_workflow_job_state_time_seconds");
        assertThat(mfsWorkflowJobStateTime).hasSampleSize(distinguishedJobEventCount);

        // Wait less than purge timeout, so no purge expected
        await(this, 900L);
        // Trigger purge indirectly by new sample
        prometheusJobEventListener.onBundleJobEvent(new BundleJobEvent("id#trigger", Job.Status.PREP, "user #0",
                "app trigger",
                new Date(0), new Date(10)));
        MetricFamilySamples purgeDuration = getMetricFamilySamples(PrometheusJobEventListener.METRIC_PURGE_DURATION);
        assertThat(purgeDuration).hasTypeOfSummary().hasSampleCountValue(0);

        // Expected purge now : 900ms + 150ms > 1000ms
        await(this, 150L);

        // Trigger purge indirectly by new sample
        prometheusJobEventListener.onBundleJobEvent(new BundleJobEvent("id#trigger", Job.Status.PREP, "user #0",
                "app trigger",
                new Date(0), new Date(10)));
        purgeDuration = getMetricFamilySamples(PrometheusJobEventListener.METRIC_PURGE_DURATION);
        assertThat(purgeDuration).hasTypeOfSummary().hasSampleCountValue(1);

        // Expect only trigger event
        mfsWorkflowJobTotal = getMetricFamilySamples("oozie_workflow_job_total");
        assertThat(mfsWorkflowJobTotal).hasSampleSize(1);
        mfsWorkflowJobStateTime = getMetricFamilySamples("oozie_workflow_job_state_time_seconds");
        assertThat(mfsWorkflowJobStateTime).hasSampleSize(1);
        mfsWorkflowJobDuration = getMetricFamilySamples(PrometheusJobEventListener.METRIC_PURGE_DURATION);
        assertThat(mfsWorkflowJobDuration).hasSampleSize(2); // Summary: 1x sum + 1x count
        mfsWorkflowJobDuration = getMetricFamilySamples(PrometheusJobEventListener.METRIC_PURGED_ITEMS);
        assertThat(mfsWorkflowJobDuration).hasSampleSize(1);

        prometheusJobEventListener.destroy();
    }


    static class RandomProducerThread extends Thread {
        private final PrometheusJobEventListener jobEventListener;
        private AtomicBoolean doStop = new AtomicBoolean();
        private static final Random RANDOM = new Random();
        private final AtomicInteger eventCounter;

        RandomProducerThread(PrometheusJobEventListener jobEventListener, AtomicInteger eventCounter) {
            this.jobEventListener = jobEventListener;
            this.eventCounter = eventCounter;
        }

        public void doStop() {
            doStop.set(true);
        }

        public void run() {
            doStop.set(false);
            while (!doStop.get()) {
                // Create random job event
                int random = RANDOM.nextInt(3);
                long start0 = System.currentTimeMillis();
                long end0 = start0 + 100L;
                String appName = "app#" + getName() + "#" + eventCounter.getAndIncrement();
                switch (random) {
                    case 0:
                        final BundleJobEvent jobEvent = new BundleJobEvent("id#trigger", Job.Status.PREP, "user #0",
                                appName,
                                new Date(start0), new Date(end0));
                        jobEventListener.onBundleJobEvent(jobEvent);
                        break;
                    case 1:
                        final CoordinatorActionEvent coordinatorActionEvent = new CoordinatorActionEvent("id#0", "parentId",
                                CoordinatorAction.Status.READY, "user #0",
                                appName,
                                null, new Date(start0),
                                "missing deps");
                        coordinatorActionEvent.setEndTime(new Date(end0));
                        jobEventListener.onCoordinatorActionEvent(coordinatorActionEvent);
                        break;
                    case 2:
                        final CoordinatorJobEvent coordinatorJobEvent = new CoordinatorJobEvent("id#0", "parentId",
                                Job.Status.FAILED, "user #0",
                                appName,
                                new Date(start0), new Date(end0));
                        jobEventListener.onCoordinatorJobEvent(coordinatorJobEvent);
                        break;
                    default:
                        throw new IllegalStateException("No job event");
                }
                // Sleep 10ms
                await(this, 10L);
            }
        }
    }

    private static void await(Object monitor, long delayMs) {
        final long start = System.currentTimeMillis();
        try {
            while (System.currentTimeMillis() - start < delayMs) {
                synchronized (monitor) {
                    monitor.wait(delayMs);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
