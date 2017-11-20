package org.apache.beam.sdk.extensions.metric;

import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonMarshaller implements Marshaller {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public String marshall(MetricQueryResults metricQueryResults) {
    try {
      return objectMapper.writeValueAsString(metricQueryResults);
    } catch (Exception e) {
      return "";
    }
  }

}
