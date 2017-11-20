package org.apache.beam.sdk.extensions.metric;

/**
 * Write metric dta to a given backend.
 */
public interface Sink {

  void write(Object rawData) throws Exception;

}
