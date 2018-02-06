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

import com.google.auto.service.AutoService;

import java.io.Serializable;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * An interface for all IO adapter services provided for XML DSL . The service provider must
 * implement this interface and annotate it with {@link AutoService}. {@link IOAdapterFactory} class
 * uses this interface identify all services available in the class path and load into the
 * IORegistryservice.
 */
public interface IOAdapter<T, V> extends Serializable {

  /** This method returns the Scheme of the IO adapters. */
  String getHandlerScheme();

  /**
   * A method to initialize the Input Adapter. This method expect an {@link InputType} instance that
   * contains all required metadata to initialize the input adapter.
   */
  void init(@Nonnull InputType type);

  /**
   * A method to initialize the Input Adapter. This method expect an {@link Output} instance that
   * contains all required metadata to initialize the output adapter.
   */
  void init(@Nonnull OutputType type);

  /**
   * This method is responsible for locating an IO adapter that matching with supplied URI scheme.
   * Every IO adapter service must have a unique URI scheme. This method performs a search using the
   * URI scheme on IOAdapter registry to locate the matching IOAdapter implementation
   */
  boolean understand(@Nonnull BeamUri uri);

  /** A method that performs reads from data sources. */
  @SuppressWarnings("rawtypes")
  PTransform read();

  /** A method to perform the writes to data sinks. */
  @SuppressWarnings("rawtypes")
  PTransform write();

  /**
   * A utility method to transform the input data into a chosen output format. The output format can
   * be defined via URI for example kafka://topic-name?format=avro or
   * hdfs://location1/data-locaton?format=parquet. The format can be defined as a output attribute
   * format.
   */
  PCollection<?> format(@SuppressWarnings("rawtypes") PCollection data);
}
