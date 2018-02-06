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

import com.google.auto.service.AutoService;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.exception.PipelineDefinitionException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.StepType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.SupportedType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


/** A concrete implementation for ParDo type Functions. */
@AutoService(ProcessExecutor.class)
public class ParDoHandler<T, V> extends AbstractStepHandler<T, V> {

  /** Generated Serial Version ID. */
  private static final long serialVersionUID = -4660017432493403419L;

  /**
   * This method responsible for setup the basic configurations for executing the step functions.
   */
  public ParDoHandler<T, V> assign(StepType metadata, PCollection<T> input) {
    super.assign(metadata, input);
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public PCollection<V> apply() throws PipelineDefinitionException {
    boolean assignableFrom = DoFn.class.isAssignableFrom(getFunClass());
    if (!assignableFrom) {
      throw new PipelineDefinitionException(
          "The handler class "
              + getFunClass().getName()
              + "defined in the flow xml file is not valid beam DoFn type.");
    }
    return getInput().apply(ParDo.of((DoFn<T, V>) createInstance()));
  }

  @Override
  public boolean matched(SupportedType type) {
    return type.equals(SupportedType.PAR_DO);
  }
}
