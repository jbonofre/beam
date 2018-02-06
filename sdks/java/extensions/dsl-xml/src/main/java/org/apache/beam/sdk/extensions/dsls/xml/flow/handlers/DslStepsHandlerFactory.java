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
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * This class is responsible for creating different types of Step Handler types based on the
 * request. User must declare a type of function in the flow step DSL definition.
 */
public final class DslStepsHandlerFactory implements Serializable {

  /** Generated serial version id. */
  private static final long serialVersionUID = -1119972425757196467L;
  /**
   * This method creates instance of data flow step handler class for different type data flow
   * operation supported by the beam.The supported Operations are {@link ParDo}},{@link
   * GroupByKey},{@link org.apache.beam.sdk.transforms.join.CoGroupByKey}, {@link Combine}, and
   * {@link Count}, and Composite. Composite type steps allows to define a custom composite
   * transform operation that bundles more than one transforms operations in a single class. Using
   * composite transforms allows for easy reuse,modular testing, and an improved monitoring
   * experience.
   *
   * @param metadata contains the metadata of data flow step.
   * @return Returns an instance of relevant handler class of type {@link AbstractStepHandler}based
   *     on user input.
   * @throws PipelineDefinitionException
   */
  public static <T, V> PCollection<V> createAndApply(StepType metadata, final PCollection<T> input)
      throws PipelineDefinitionException {
    ProcessExecutor<T, V> create = create(metadata);
    return create.assign(metadata, input).apply();
  }

  /**
   * This method creates instance of data flow step handler class for different type data flow
   * operation supported by the beam.The supported Operations are {@link ParDo}},{@link
   * GroupByKey},{@link org.apache.beam.sdk.transforms.join.CoGroupByKey}, {@link Combine}, and
   * {@link Count}, and Composite. Composite type steps allows to define a custom composite
   * transform operation that bundles more than one transforms operations in a single class. Using
   * composite transforms allows for easy reuse,modular testing, and an improved monitoring
   * experience.
   *
   * @param metadata contains the metadata of data flow step.
   * @param input is an instance of {@link PCollection} contains input values
   * @return Returns an instance of relevant handler class of type {@link AbstractStepHandler}based
   *     on user input.
   * @throws PipelineDefinitionException
   */
  private static <T, V> ProcessExecutor<T, V> create(StepType metadata)
      throws PipelineDefinitionException {
    switch (metadata.getType()) {
      case PAR_DO:
        return new ParDoHandler<T, V>();
      case MAP:
        return new MapElementHandler<T, V>();
      case COMPOSITE:
        return new CompositeStepHandler<T, V>();
      default:
        throw new UnsupportedOperationException(
            "Requested operation type " + metadata.getType() + "does not supported.");
    }
  }
}
