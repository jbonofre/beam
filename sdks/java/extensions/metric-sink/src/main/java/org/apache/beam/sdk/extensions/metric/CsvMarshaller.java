package org.apache.beam.sdk.extensions.metric;

import org.apache.beam.sdk.metrics.*;

public class CsvMarshaller implements Marshaller {

  @Override
  public String marshall(MetricQueryResults metricQueryResults) {
    StringBuffer buffer = new StringBuffer();
    for (MetricResult<Long> counter : metricQueryResults.counters()) {
      MetricName name = counter.name();
      buffer.append(name.namespace())
          .append("/")
          .append(name.name())
          .append(" (")
          .append(counter.step())
          .append(") = ")
          .append(counter.committed())
          .append(" (")
          .append(counter.attempted())
          .append(")")
          .append("\n");
    }
    for (MetricResult<GaugeResult> gauge : metricQueryResults.gauges()) {
      MetricName name = gauge.name();
      buffer.append(name.namespace())
          .append("/")
          .append(name.name())
          .append(" (")
          .append(gauge.step())
          .append(") =")
          .append(gauge.committed().value())
          .append(" @ ").append(gauge.committed().timestamp().toString())
          .append(" (").append(gauge.attempted().value())
          .append(" @ ").append(gauge.attempted().timestamp().toString())
          .append(")")
          .append("\n");
    }
    return buffer.toString();
  }

}
