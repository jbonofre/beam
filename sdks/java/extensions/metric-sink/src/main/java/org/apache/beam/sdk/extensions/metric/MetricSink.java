package org.apache.beam.sdk.extensions.metric;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;

/**
 * Extension polling {@link PipelineResult} to get metrics, marshal those metrics in a format,
 * and store to a backend.
 */
public class MetricSink {

  private final PipelineResult pipelineResult;
  private final Marshaller marshaller;
  private final Sink sink;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public MetricSink(PipelineResult pipelineResult, Marshaller marshaller, Sink sink, long period) {
    this.pipelineResult = pipelineResult;
    this.marshaller = marshaller;
    this.sink = sink;
    scheduler.scheduleAtFixedRate(new PollingThread(), 0, period, TimeUnit.SECONDS);
  }

  private class PollingThread implements Runnable {

    @Override
    public void run() {
      try {
        MetricResults metricResults = pipelineResult.metrics();
        MetricQueryResults metricQueryResults = metricResults
            .queryMetrics(MetricsFilter.builder().build());
        Object rawData = marshaller.marshall(metricQueryResults);
        sink.write(rawData);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

}
