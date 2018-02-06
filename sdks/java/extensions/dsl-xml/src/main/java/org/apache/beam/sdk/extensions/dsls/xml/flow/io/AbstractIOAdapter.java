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

package org.apache.beam.sdk.extensions.dsls.xml.flow.io;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities;

/** An abstract class for all IO adapter. */
@SuppressWarnings("rawtypes")
public abstract class AbstractIOAdapter implements IOAdapter {

  /** Generated serial version id. */
  private static final long serialVersionUID = -1137919370828589395L;

  private BeamUri decodedURI;
  private Map<String, String> properties = null;

  @Override
  public boolean understand(@Nonnull BeamUri uri) {
    return uri.getScheme().equalsIgnoreCase(getHandlerScheme()) ? true : false;
  }

  @Override
  public abstract String getHandlerScheme();

  @Override
  public void init(@Nonnull InputType type) {
    decodedURI = Utilities.getDecodedURI(type.getUri());
    properties = Utilities.getProperties(type.getAttributes().getAttribute());
  }

  @Override
  public void init(@Nonnull OutputType type) {
    decodedURI = Utilities.getDecodedURI(type.getUri());
    properties = Utilities.getProperties(type.getAttributes().getAttribute());
  }

  public BeamUri getDecodedURI() {
    return decodedURI;
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
