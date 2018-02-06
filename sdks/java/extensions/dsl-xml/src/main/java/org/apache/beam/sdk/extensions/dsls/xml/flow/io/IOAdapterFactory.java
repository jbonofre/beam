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

import java.io.Serializable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * This class is responsible for selecting choosing appropriate Input, and output adapters supplied,
 * initialize it correctly, and apply it to data pipeline. All IO Adapters must implement the
 * interface IO adapter see {@link IOAdapter} and annotate it with @Autoservice. This factory class
 * accepts input and out metadata from the caller and uses URI scheme to identify the requested
 * IOAdapter service from the IOadatper registry. After locating matched IO adapter it applies
 * either read or write operations dependents on the factory method selected.
 *
 * <p>This class exposes two factory methods and those are listed below
 *
 * <ul>
 *   <li>{@link #createInputAdapterAndApply(Pipeline, InputType)}
 *   <li>{@link #createOutputAdapterAndApply(OutputType, PCollection)}
 * </ul>
 *
 * The {@link #createInputAdapterAndApply(Pipeline, InputType)} is responsible for locating,
 * initializing the IOAdapter and execute the read operation on chosen IO adapter. The {@link
 * #createOutputAdapterAndApply(OutputType, PCollection)} is responsible for locating a right IO
 * adapter and execute the write operation.</p>
 */
public final class IOAdapterFactory implements Serializable {

  /** Generated Serial version id. */
  private static final long serialVersionUID = -3674479601048000314L;

  private IOAdapterFactory() {}

  /**
   * The method is responsible for locating, initialize the Input Adapter and execute the read
   * operation on chosen IO adapter.
   *
   * @param pipline A reference to the current {@link Pipeline} that manages a directed acyclic
   *     graph of {@link PTransform PTransforms}.
   * @param input A reference of the input metadata.
   * @return the instance of {@link PCollection} contains the data read from the selected source.
   */
  @SuppressWarnings("unchecked")
  public static PCollection<?> createInputAdapterAndApply(Pipeline pipline, InputType input) {
    @SuppressWarnings("rawtypes")
    IOAdapter ioAdapterService = IOServiceRegsitry.getIOAdapterService(input.getUri());
    ioAdapterService.init(input);
    return (PCollection<?>) pipline.apply(ioAdapterService.read());
  }

  /**
   * The method is responsible for locating, initialize the Output adapter and execute the write
   * operation on chosen IO adapter.
   * @param output A reference of the output metadata.
   * @return the instance of {@link PCollection} contains the data read from the selected source.
   */
  @SuppressWarnings("unchecked")
  public static PDone createOutputAdapterAndApply(
      OutputType output, @SuppressWarnings("rawtypes") PCollection data) {
    @SuppressWarnings("rawtypes")
    IOAdapter ioAdapterService = IOServiceRegsitry.getIOAdapterService(output.getUri());
    ioAdapterService.init(output);
    return (PDone) ioAdapterService.format(data).apply(ioAdapterService.write());
  }
}
