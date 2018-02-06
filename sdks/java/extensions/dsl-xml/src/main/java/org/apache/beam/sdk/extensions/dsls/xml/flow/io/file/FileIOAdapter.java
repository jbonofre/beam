/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.dsls.xml.flow.io.file;

import com.google.auto.service.AutoService;

import javax.annotation.Generated;
import javax.annotation.Nonnull;

import org.apache.beam.sdk.extensions.dsls.xml.flow.io.AbstractIOAdapter;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.IOAdapter;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.file.config.FileReaderConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.file.config.FileWriterConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.TextIO.Read;
import org.apache.beam.sdk.io.TextIO.Write;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;



/**
 * This class responsible to handle the reads text files that reads file(s) with the given filename
 * or filename pattern a file system (local or NFS file system).
 *
 * <p>This can be a local path (if running locally), filename pattern of the form {@code
 * "file://path"}
 *
 * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
 * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
 */
@AutoService(IOAdapter.class)
public class FileIOAdapter extends AbstractIOAdapter {

  /** Generated Serial version id. */
  private static final long serialVersionUID = -2575117153098957592L;

  protected FileReaderConfiguration fileReaderConiguration;
  protected FileWriterConfiguration fileWriterConfiguration;

  /**
   * @param inputType
   * @return
   */
  @Override
  public Read read() {
    return TextIO.read()
        .from(getReadPath())
        .withCompressionType(fileReaderConiguration.getCompressionType());
  }

  private String getReadPath() {
    return fileReaderConiguration.getPath();
  }

  public void init(@Nonnull InputType type) {
    super.init(type);
    fileReaderConiguration = FileReaderConfiguration.build(getProperties(), getDecodedURI());
  }

  @Override
  public void init(@Nonnull OutputType type) {
    super.init(type);
    fileWriterConfiguration = FileWriterConfiguration.build(getProperties(), getDecodedURI());
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.beam.sdk.extensions.dsls.xml.flow.io.IOAdapter#getHandlerSchema()
   */
  @Override
  public String getHandlerScheme() {
    return IOConfiguration.FileIOConfiguration.SCHEME_TYPE;
  }

  public Write write() {
    return new Builder()
        .beginWith(TextIO.write().to(getWritePath()), fileWriterConfiguration)
        .checkAndBuild();
  }

  protected String getWritePath() {
    return fileWriterConfiguration.getPath();
  }

  /** Builder to build {@link WriterHelper}. */
  @Generated("SparkTools")
  public static final class Builder {

    private Write write;
    private FileWriterConfiguration fileWriterConfiguration;

    private Builder() {}

    public Builder beginWith(Write write, FileWriterConfiguration fileWriterConfiguration) {
      this.write = write;
      this.fileWriterConfiguration = fileWriterConfiguration;
      return this;
    }

    public Write checkAndBuild() {
      if (write == null || fileWriterConfiguration == null) {
        throw new IllegalStateException("Writer is not initilized properly.");
      }
      if (fileWriterConfiguration.isHeaderEnabled()) {
        write = write.withHeader(fileWriterConfiguration.getHeader());
      }
      if (fileWriterConfiguration.isFooterEnabled()) {
        write = write.withFooter(fileWriterConfiguration.getFooter());
      }
      if (fileWriterConfiguration.isShardFileNameTemplateDefined()) {
        write = write.withShardNameTemplate(fileWriterConfiguration.getshardFileNameTemplate());
      }

      if (fileWriterConfiguration.isShardCountDefined()) {
        write = write.withNumShards(fileWriterConfiguration.getShardCounts());
      } else {
        write = write.withoutSharding();
      }
      if (fileWriterConfiguration.isSuffixDefined()) {
        write = write.withSuffix(fileWriterConfiguration.getSuffix());
      }
      if (fileWriterConfiguration.isWidnowedWriteEnabled()) {
        write = write.withWindowedWrites();
      }
      return write;
    }
  }

  /* (non-Javadoc)
   * @see org.apache.beam.sdk.extensions.dsls.xml.flow.io.IOAdapter#format
   * (org.apache.beam.sdk.values.PCollection)
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public PCollection format(PCollection data) {
    // String format = getDecodedURI().getFormat();

    TypeDescriptor<?> typeDescriptor = data.getTypeDescriptor();
    if (typeDescriptor.getType().equals(String.class)) {
      return (PCollection<String>) data;
    }
    throw new RuntimeException("Unsupport ddata format. File IO supports only text format now.");
  }
}
