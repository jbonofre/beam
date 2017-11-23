package org.apache.beam.sdk.extensions.metric;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

/**
 * Simple tests on {@link MetricSink} extension.
 */
public class MetricSinkTest {

  @Test
  public void pipelineTest() throws Exception {
    PipelineWithMetric pipeline = PipelineWithMetric.create(PipelineOptionsFactory.create(), new CsvMarshaller(), new FileSink("target/metric"), 1);
    pipeline.run();
  }

}
