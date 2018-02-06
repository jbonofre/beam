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

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities;
import org.apache.beam.sdk.io.TextIO.CompressionType;

/** This class is responsible for File reading operation configuration management. */
public class FileReaderConfiguration extends FileCommonConfiguration
    implements IOConfiguration.FileInputConfiguration {

  /**
   * A constructor this class .
   *
   * @param properties is the parameters for file read configuration.
   * @param decodedURI, A translated URI object.
   */
  FileReaderConfiguration(Map<String, String> properties, BeamUri decodedURI) {
    super(properties, decodedURI);
  }

  /** Generated Serial version id. */
  private static final long serialVersionUID = -8351807093930860293L;
  /**
   * A builder method for this class.
   *
   * @param properties Properties from the {@link InputType}
   * @return instance of this class
   */
  public static FileReaderConfiguration build(Map<String, String> properties, BeamUri decodedURI) {
    return new FileReaderConfiguration(properties, decodedURI);
  }

  public CompressionType getCompressionType() {
    return Utilities.getCompressionType(getUri(), getProperties().get(COMPRESSION_TYPE));
  }

  public String getPath() {
    return getUri().getPath();
  }
}
