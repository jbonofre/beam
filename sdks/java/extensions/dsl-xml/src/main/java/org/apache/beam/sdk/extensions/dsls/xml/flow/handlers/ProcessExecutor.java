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
package org.apache.beam.sdk.extensions.dsls.xml.flow.handlers;

import java.io.Serializable;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.exception.PipelineDefinitionException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.StepType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.SupportedType;
import org.apache.beam.sdk.values.PCollection;

/**
 * This interface for Step handler of the DSL implementation. Apache beam supports different types
 * of operations, and each of them will have its own handler class . A handle factory class will be
 * responsible for creating them on demand.
 *
 * @param <T> input data of type {@link PCollection}.
 * @param <V> output data of type {@link PCollection}.
 */
interface ProcessExecutor<T, V> extends Serializable {
  /** A method apply the operation on input {@link PCollection}. */
  PCollection<V> apply() throws PipelineDefinitionException;

  /** Method to configure the current step. */
  ProcessExecutor<T, V> assign(StepType metadata, PCollection<T> input);

  /** A method to locate the correct operation type. */
  boolean matched(SupportedType type);
}
