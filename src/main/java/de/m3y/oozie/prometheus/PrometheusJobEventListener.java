package de.m3y.oozie.prometheus;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
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
 * <b>Note:</b> Any ({@value LABEL_JOB_TYPE}, {@value LABEL_APP_NAME}, {@value LABEL_STATUS}) metric sample will be
 * automatically flushed out after either default of {@value DEFAULT_TIME_SERIES_IDLE_TTL_DAYS} (configurable
 * via System property {@value PROPERTY_TIME_SERIES_IDLE_TTL_SECONDS}) and {@value PROPERTY_PURGE_INTERVAL} to avoid filling up memory.
 */
public class PrometheusJobEventListener extends JobEventListener {
    static final String PROPERTY_TIME_SERIES_IDLE_TTL_SECONDS = "PrometheusJobEventListener.TimeSeries.Idle.TTL.seconds";
    static final String PROPERTY_PURGE_INTERVAL = "PrometheusJobEventListener.Purge.Interval.seconds";
    public static final String METRIC_PURGE_DURATION = "oozie_prometheus_job_listener_purge_duration_seconds";
    public static final String METRIC_PURGED_ITEMS = "oozie_prometheus_job_listener_purged_items_total";
    final long timeSeriesIdleTtlMs = TimeUnit.SECONDS.toMillis(configurePropertyWithDefault(
            PROPERTY_TIME_SERIES_IDLE_TTL_SECONDS, DEFAULT_TIME_SERIES_IDLE_TTL_SECONDS));
    static final long DEFAULT_TIME_SERIES_IDLE_TTL_DAYS = 3;
    static final long DEFAULT_PURGE_INTERVAL_DAYS = 1;
    private static final long DEFAULT_TIME_SERIES_IDLE_TTL_SECONDS = TimeUnit.DAYS.toSeconds(DEFAULT_TIME_SERIES_IDLE_TTL_DAYS);
    private static final long DEFAULT_PURGE_INTERVAL_SECONDS = TimeUnit.DAYS.toSeconds(DEFAULT_PURGE_INTERVAL_DAYS);
    final long purgeIntervalMs = TimeUnit.SECONDS.toMillis(
            configurePropertyWithDefault(PROPERTY_PURGE_INTERVAL, DEFAULT_PURGE_INTERVAL_SECONDS));

    private static final XLog LOG = XLog.getLog(PrometheusJobEventListener.class);

    private static final String LABEL_JOB_TYPE = "job_type";
    private static final String LABEL_APP_NAME = "app_name";
    private static final String LABEL_STATUS = "status";

    private static long configurePropertyWithDefault(String systemPropertyName, long defaultValue) {
        final String property = System.getProperty(systemPropertyName);
        if (null != property && !property.isEmpty()) {
            try {
                return Long.parseLong(property);
            } catch (NumberFormatException e) {
                LOG.warn("Can not parse system property " + systemPropertyName + " with value <"
                        + property + "> - using default " + defaultValue + "[s]");
            }
        }
        return defaultValue;
    }

    final Gauge workflowJobDuration = Gauge.build()
            .name("oozie_workflow_job_duration_seconds")
            .help("Duration of completed jobs")
            .labelNames(LABEL_JOB_TYPE, LABEL_APP_NAME, LABEL_STATUS)
            .create();
    final Gauge workflowJobEventTimestamp = Gauge.build()
            .name("oozie_workflow_job_state_time_seconds")
            .help("Timestamp of completed job state changes")
            .labelNames(LABEL_JOB_TYPE, LABEL_APP_NAME, LABEL_STATUS)
            .create();
    final Counter workflowJobTotal = Counter.build()
            .name("oozie_workflow_job_total")
            .help("Count of completed job state changes")
            .labelNames(LABEL_JOB_TYPE, LABEL_APP_NAME, LABEL_STATUS)
            .create();

    // Purge stats
    final Summary purgeDurations = Summary.build()
            .name(METRIC_PURGE_DURATION)
            .help("Duration of purge operations")
            .create();
    final Counter purgedSamplesTotal = Counter.build()
            .name(METRIC_PURGED_ITEMS)
            .help("Count of purged samples (approximated)")
            .create();

    /**
     * Tracks samples last modification timestamp, and purges sample time series from collectors if idle.
     */
    class SamplesPurger {
        private final AtomicLong lastPurgeTimestamp = new AtomicLong(System.currentTimeMillis());
        private Map<List<String>, SampleUpdateTimestamp> sampleLabelsCache = new ConcurrentHashMap<>();

        void updateTimestamp(String[] labels) {
            final List<String> key = Arrays.asList(labels);
            sampleLabelsCache.computeIfAbsent(key, SampleUpdateTimestamp::new).updateTimestampToNow();

            triggerPurge();
        }

        private void triggerPurge() {
            final long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastPurgeTimestamp.get() > purgeIntervalMs) {
                if (lastPurgeTimestamp.compareAndSet(lastPurgeTimestamp.get(), currentTimeMillis)) {
                    // TRack purge time and counts as metric
                    try (final Summary.Timer timer = purgeDurations.startTimer()) {
                        final int samplesBeforePurgeCount = sampleLabelsCache.size();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Purging old samples. Current samples size is " + samplesBeforePurgeCount);
                        }
                        long maxTime = currentTimeMillis - timeSeriesIdleTtlMs;
                        sampleLabelsCache.values().stream()
                                .filter(s -> s.lastUpdateTimestamp < maxTime)
                                .forEach(this::purgeSample);

                        final int samplesAfterPurgeCount = sampleLabelsCache.size();
                        purgedSamplesTotal.inc(
                                Math.max(0, samplesBeforePurgeCount - samplesAfterPurgeCount /* can be negative */));
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("After purging, samples size is " + samplesAfterPurgeCount);
                        }
                    }
                }
            }
        }

        private void purgeSample(SampleUpdateTimestamp sampleUpdate) {
            // Clean cache
            sampleLabelsCache.remove(sampleUpdate.labels);

            // Remove from metrics, to terminate time series
            String[] labelsAsArray = sampleUpdate.labels.toArray(new String[3]);
            workflowJobDuration.remove(labelsAsArray);
            workflowJobEventTimestamp.remove(labelsAsArray);
            workflowJobTotal.remove(labelsAsArray);
        }
    }

    /**
     * Tracks sample update by identifying a sample by its labels and keeping recent update timestamp.
     */
    static class SampleUpdateTimestamp {
        final List<String> labels; // Identifies sample aka time series
        long lastUpdateTimestamp = System.currentTimeMillis();

        SampleUpdateTimestamp(List<String> labels) {
            this.labels = labels;
        }

        void updateTimestampToNow() {
            lastUpdateTimestamp = System.currentTimeMillis();
        }
    }

    final SamplesPurger purger = new SamplesPurger();

    @Override
    public void init(Configuration configuration) {
        LOG.info("Initializing Prometheus Job Event Listener");
        workflowJobDuration.register();
        workflowJobEventTimestamp.register();
        workflowJobTotal.register();
        purgeDurations.register();
        purgedSamplesTotal.register();
    }

    @Override
    public void destroy() {
        LOG.info("Destroying Prometheus Job Event Listener");
        CollectorRegistry.defaultRegistry.unregister(workflowJobDuration);
        CollectorRegistry.defaultRegistry.unregister(workflowJobEventTimestamp);
        CollectorRegistry.defaultRegistry.unregister(workflowJobTotal);
        CollectorRegistry.defaultRegistry.unregister(purgeDurations);
        CollectorRegistry.defaultRegistry.unregister(purgedSamplesTotal);
    }

    @Override
    public void onWorkflowJobEvent(WorkflowJobEvent workflowJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received workflow job event " + workflowJobEvent);
        }

        final String statusName = workflowJobEvent.getStatus().name();
        final String[] labels = getLabels(workflowJobEvent, statusName);
        updateMetrics(workflowJobEvent, labels);
    }


    @Override
    public void onWorkflowActionEvent(WorkflowActionEvent workflowActionEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received workflow action event " + workflowActionEvent);
        }

        final String statusName = workflowActionEvent.getStatus().name();
        final String[] labels = getLabels(workflowActionEvent, statusName);
        updateMetrics(workflowActionEvent, labels);
    }

    @Override
    public void onCoordinatorJobEvent(CoordinatorJobEvent coordinatorJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received coordinator job event " + coordinatorJobEvent);
        }

        final String statusName = coordinatorJobEvent.getStatus().name();
        final String[] labels = getLabels(coordinatorJobEvent, statusName);
        updateMetrics(coordinatorJobEvent, labels);
    }

    @Override
    public void onCoordinatorActionEvent(CoordinatorActionEvent coordinatorActionEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received coordinator action event " + coordinatorActionEvent);
        }

        final String statusName = coordinatorActionEvent.getStatus().name();
        final String[] labels = getLabels(coordinatorActionEvent, statusName);
        updateMetrics(coordinatorActionEvent, labels);
    }

    @Override
    public void onBundleJobEvent(BundleJobEvent bundleJobEvent) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received bundle job event " + bundleJobEvent);
        }

        final String statusName = bundleJobEvent.getStatus().name();
        final String[] labels = getLabels(bundleJobEvent, statusName);
        updateMetrics(bundleJobEvent, labels);
    }

    private void updateMetrics(JobEvent jobEvent, String[] labels) {
        purger.updateTimestamp(labels);
        updateDuration(jobEvent, labels);
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
