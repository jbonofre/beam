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

import java.io.Serializable;
import java.util.Map;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration.FileIOConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;

/**
 * A common file configuration management class. This class manages the common properties of both
 * file read/write operations.
 */
abstract class FileCommonConfiguration implements FileIOConfiguration, Serializable {

  /** Generated Serial version id. */
  private static final long serialVersionUID = -8351807093930860293L;

  /** File IO adapter configuration properties. */
  private final Map<String, String> properties;
  /** A translated URI reference. */
  private BeamUri uri;

  /**
   * A constructor for this class.
   *
   * @param decodedUri is a instance of decoded uri.
   */
  FileCommonConfiguration(Map<String, String> properties, BeamUri decodedUri) {
    this.properties = properties;
    this.uri = decodedUri;
  }

  protected BeamUri getUri() {
    return uri;
  }

  /** This method returns a {@link Map} contains all configuration for File IO. */
  protected Map<String, String> getProperties() {
    return properties;
  }
}
