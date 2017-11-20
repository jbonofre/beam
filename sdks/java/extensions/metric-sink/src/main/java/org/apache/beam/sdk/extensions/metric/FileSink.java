package org.apache.beam.sdk.extensions.metric;

import java.io.File;
import java.io.FileWriter;

public class FileSink implements Sink {

  private final File file;

  public FileSink(String path) {
    this.file = new File(path);
  }

  @Override
  public void write(Object rawData) {
    try (FileWriter writer = new FileWriter(file)) {
      String data = (String) rawData;
      writer.write(data);
    } catch (Exception e) {
      // TODO
    }
  }

}
