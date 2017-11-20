package org.apache.beam.sdk.extensions.metric;

import org.apache.beam.sdk.metrics.MetricQueryResults;

/**
 * A marshaller to convert MetricQueryResult as an output format.
 */
public interface Marshaller {

  Object marshall(MetricQueryResults metricQueryResults);

}
