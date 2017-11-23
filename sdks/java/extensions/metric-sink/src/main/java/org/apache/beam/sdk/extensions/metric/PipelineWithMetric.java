package org.apache.beam.sdk.extensions.metric;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class PipelineWithMetric extends Pipeline {

  private final Marshaller marshaller;
  private final Sink sink;
  private final long period;

  public static PipelineWithMetric create() {
    return new PipelineWithMetric(null, null, null, -1);
  }

  public static PipelineWithMetric create(PipelineOptions pipelineOptions) {
    return new PipelineWithMetric(pipelineOptions, null, null, -1);
  }

  public static PipelineWithMetric create(PipelineOptions pipelineOptions, Marshaller marshaller, Sink sink, long period) {
    return new PipelineWithMetric(pipelineOptions, marshaller, sink, period);
  }

  private PipelineWithMetric(PipelineOptions pipelineOptions, Marshaller marshaller, Sink sink, long period) {
    super(pipelineOptions);
    this.marshaller = marshaller;
    this.sink = sink;
    this.period = period;
  }

  @Override
  public PipelineResult run() {
    PipelineResult pipelineResult = super.run();
    MetricSink metricSink = new MetricSink(pipelineResult, marshaller, sink, period);
    return pipelineResult;
  }

}
