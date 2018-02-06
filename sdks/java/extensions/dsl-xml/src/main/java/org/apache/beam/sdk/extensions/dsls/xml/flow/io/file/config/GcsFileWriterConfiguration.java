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

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;

/** A configuration management class for GSF writer. */
public class GcsFileWriterConfiguration extends FileWriterConfiguration {

  GcsFileWriterConfiguration(Map<String, String> properties, BeamUri decodedURI) {
    super(properties, decodedURI);
    // TODO Auto-generated constructor stub
  }

  /** Generated Serial version id. */
  private static final long serialVersionUID = 3279115797355243303L;

  public String getPath() {
    return getUri().getScheme() + ":" + getUri().getPath();
  }
  /**
   * A builder method for this class.
   *
   * @param properties Properties from the {@link OutputType}
   * @return instance of this class
   */
  public static GcsFileWriterConfiguration build(
      Map<String, String> properties, BeamUri decodedURI) {
    return new GcsFileWriterConfiguration(properties, decodedURI);
  }
}
