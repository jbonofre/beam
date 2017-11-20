package org.apache.beam.sdk.extensions.metric;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.junit.Test;

/**
 * Simple tests on {@link MetricSink} extension.
 */
public class MetricSinkTest {

  @Test
  public void pipelineTest() throws Exception {
    Pipeline pipeline = Pipeline.create();
    PipelineResult result = pipeline.run();
    MetricSink metricSink = new MetricSink(result, new CsvMarshaller(),
        new FileSink("target/metric"), 1);
  }

}
