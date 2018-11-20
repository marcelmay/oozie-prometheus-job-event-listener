package de.m3y.oozie.prometheus;

import java.util.List;
import java.util.Objects;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import org.assertj.core.api.AbstractAssert;

/**
 * AssertJ support for MetricFamilySamples (EXPERIMENTAL)
 */
public class MetricFamilySamplesAssert extends AbstractAssert<MetricFamilySamplesAssert, MetricFamilySamples> {
    public MetricFamilySamplesAssert(MetricFamilySamples metricFamilySamples) {
        super(metricFamilySamples, MetricFamilySamplesAssert.class);
    }

    public static MetricFamilySamplesAssert assertThat(MetricFamilySamples actual) {
        return new MetricFamilySamplesAssert(actual);
    }

    public MetricFamilySamplesAssert hasName(String name) {
        isNotNull();

        if (!Objects.equals(actual.name, name)) {
            failWithMessage("Expected MetricFamilySamples's name to be <%s> but was <%s>", name, actual.name);
        }

        return this;
    }

    public MetricFamilySamplesAssert hasType(Collector.Type type) {
        isNotNull();

        if (!Objects.equals(actual.type, type)) {
            failWithMessage("Expected MetricFamilySamples's type to be <%s> but was <%s>", type, actual.type);
        }

        return this;
    }

    public MetricFamilySamplesAssert hasAnySamples() {
        isNotNull();

        if (null == actual.samples || actual.samples.isEmpty()) {
            failWithMessage("Expected MetricFamilySamples's samples <%s> but was null or empty", actual.samples);
        }

        return this;
    }

    public MetricFamilySamplesAssert hasSample(List<String> labelNames) {
        return hasSample(labelNames, null, null, null);
    }

    public MetricFamilySamplesAssert hasSample(List<String> labelNames, List<String> labelValues) {
        return hasSample(labelNames, labelValues, null, null);
    }

    public MetricFamilySamplesAssert hasSample(List<String> labelNames, List<String> labelValues, Double value) {
        return hasSample(labelNames, labelValues, value, null);
    }

    public MetricFamilySamplesAssert hasSample(List<String> labelNames, List<String> labelValues, Double value, Double errorTolerance) {
        isNotNull();
        hasAnySamples();

        // Check if sample exist
        MetricFamilySamples.Sample sample = findSample(labelNames, labelValues);
        if (null == sample) {
            failWithMessage("Expected MetricFamilySamples's sample for <%s> in samples <%s>  ",
                    joinLabelNamesAndValues(labelNames, labelValues),
                    actual.samples);
        }

        // Check sample value, if provided
        if (null != value) {
            if (null != errorTolerance) { // Optional deviation error
                if (Math.abs(sample.value - value) > errorTolerance) {
                    failWithMessage("Expected MetricFamilySamples.Sample value <%f> +/- <%f>  but got <%f> (delta = %f)",
                            value,
                            errorTolerance,
                            sample.value,
                            sample.value - value);
                }
            } else {
                if (sample.value != value) {
                    failWithMessage("Expected MetricFamilySamples.Sample value <%f> but got <%f>",
                            value,
                            sample.value);
                }
            }
        }

        return this;
    }


    private MetricFamilySamples.Sample findSample(List<String> labelNames, List<String> labelValues) {
        for (MetricFamilySamples.Sample sample : actual.samples) {
            if (sample.labelNames.equals(labelNames)) {
                if (null == labelValues) {
                    return sample; // First one found, by label name only
                }
                if (sample.labelValues.equals(labelValues)) {
                    return sample;
                }
            }
        }
        return null;
    }

    private String joinLabelNamesAndValues(List<String> labelNames, List<String> labelValues) {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < Math.max(labelNames.size(), labelValues.size()); i++) {
            if (buf.length() > 0) {
                buf.append(", ");
            }
            buf.append(i < labelNames.size() ? labelNames.get(i) : "");
            buf.append('=');
            buf.append(i < labelValues.size() ? labelValues.get(i) : "");
        }
        return buf.toString();
    }

}
