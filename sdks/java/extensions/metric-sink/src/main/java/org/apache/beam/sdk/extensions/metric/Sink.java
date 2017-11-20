package org.apache.beam.sdk.extensions.metric;

public interface Sink {

  void write(Object rawData);

}
