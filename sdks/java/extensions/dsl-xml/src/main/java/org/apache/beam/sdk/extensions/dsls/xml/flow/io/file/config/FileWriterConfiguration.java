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
package org.apache.beam.sdk.extensions.dsls.xml.flow.io.file.config;

import java.util.Map;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration.FileOutputConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities;
/** A configuration management class for the File writer. */
public class FileWriterConfiguration extends FileCommonConfiguration
    implements FileOutputConfiguration {

  /** Generated serial version id. */
  private static final long serialVersionUID = 3950434407985469113L;
  /**
   * A constructor this class .
   *
   * @param properties is the parameters for file writer configuration.
   * @param decodedURI, A translated URI object.
   */
  FileWriterConfiguration(Map<String, String> properties, BeamUri decodedURI) {
    super(properties, decodedURI);
  }
  /**
   * A builder method for this class.
   *
   * @param properties Properties from the {@link OutputType}
   * @return instance of this class
   */
  public static FileWriterConfiguration build(Map<String, String> properties, BeamUri decodedURI) {
    return new FileWriterConfiguration(properties, decodedURI);
  }
  /** Returns the file header if defined otherwise null. */
  public String getHeader() {
    return getProperties().get(HEADER);
  }
  /** Returns the file header if defined otherwise null. */
  public boolean isHeaderEnabled() {
    return getHeader() != null;
  }

  /** Returns the file footer if defined otherwise null. */
  public String getFooter() {
    return getProperties().get(FOOTER);
  }
  /** Returns the file footer if defined otherwise null. */
  public boolean isFooterEnabled() {
    return getFooter() != null;
  }

  /** Returns boolean value true if the shards defined otherwise false. */
  public boolean isShardCountDefined() {
    Integer shards = getShardCounts();
    return shards != null && shards > 0;
  }

  /** Returns number shards if it defined otherwise no shards. */
  public int getShardCounts() {
    return Utilities.getIntProperty(getProperties(), NUM_SHARDS);
  }

  /** returns true is the shard file name template defined otherwise false. */
  public boolean isShardFileNameTemplateDefined() {
    return getshardFileNameTemplate() != null;
  }

  /** Returns the user defined shard file name template. */
  public String getshardFileNameTemplate() {
    return getProperties().get(SHARD_FILE_NAME_TEMPLATE);
  }

  /** Returns true if the suffix is defined for the output file otherwise false. */
  public boolean isSuffixDefined() {
    return getSuffix() != null;
  }

  /** Returns the file name suffix. */
  public String getSuffix() {
    return getProperties().get(SUFFIX);
  }

  /** Is the windowed writes enabled returns true otherwise false. */
  public boolean isWidnowedWriteEnabled() {
    final String windowWriteValue = getProperties().get(WINDOWED_WRITES);
    return windowWriteValue != null && windowWriteValue.equalsIgnoreCase("true");
  }

  public String getPath() {
    return getUri().getPath();
  }
}
