package org.apache.beam.sdk.extensions.metric;

import java.io.File;
import java.io.FileWriter;

/**
 * Simple metric sink writing metric data to a file.
 */
public class FileSink implements Sink {

  private final File file;

  public FileSink(String path) {
    this.file = new File(path);
  }

  @Override
  public void write(Object rawData) throws Exception {
    try (FileWriter writer = new FileWriter(file)) {
      String data = (String) rawData;
      writer.write(data);
    }
  }

}
