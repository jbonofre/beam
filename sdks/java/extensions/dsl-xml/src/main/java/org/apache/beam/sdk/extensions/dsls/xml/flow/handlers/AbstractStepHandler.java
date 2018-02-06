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

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.exception.PipelineDefinitionException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.StepType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities;
import org.apache.beam.sdk.values.PCollection;

/**
 * This abstract class implements few common functions required for all data processing step handler
 * classes those implements {@link ProcessExecutor} class.
 *
 * <p>See the example below for defining new type of Step Handler class.
 *
 * <pre>{@code
 * public class MyStepHandler<T, V> extends AbstractStepHandler<T, V>{
 *
 *   @Override
 *   public MyStepHandler<T,V> assign(StepType metadata,  PCollection<T> input) {
 *     super.assign(metadata,  input);
 *     return this;
 *   }
 *   @Override
 *   public PCollection<V> apply()throws PipelineDefinitionException {
 *     // implementation goes here.
 *   }
 *   @Override
 *   public boolean matched(SupportedType type) {
 *      return type.equals(SupportedType.MAP);
 *   }
 *
 * }
 * }</pre>
 */
public abstract class AbstractStepHandler<T, V> implements ProcessExecutor<T, V> {

  /*
   * Generated serial version id.
   * */
  private static final long serialVersionUID = 1480298640913110508L;
  /*
   * A step function class that defined in the flow DSL.
   */
  private Class<?> funClass;
  /*
   * A {@link PCollection PCollection&lt;T&gt;} is an immutable collection of values of type
   * {@code T}.  A {@link PCollection} can contain either a bounded or unbounded
   * number of elements. A reference to the input data collection
   */
  private PCollection<T> input;
  /*
   *A reference to StepType class  which contains the mapped values from the DSL.
   **/
  private StepType metadata;
  /**
   * This helper method to prepare the step metadata before applying it on the input data {@link
   * PCollection}.
   *
   * @param metadata is an instance of {@link StepType} contains the metadata of flow DSL step. This
   *     metadata contains the relevant information for constructing a data processing step.
   * @param input is a collection of input data to the data processing step.
   */
  @Override
  public ProcessExecutor<T, V> assign(StepType metadata, PCollection<T> input) {
    this.metadata = metadata;
    this.input = input;
    funClass = Utilities.getClass(metadata);
    return this;
  }

  /** An access method for current step's metadata. */
  public StepType getMetadata() {
    return metadata;
  }

  /** An access method for current step's used defined function class. */
  public Class<?> getFunClass() {
    return funClass;
  }

  /** An access method for current step's input data. */
  public PCollection<T> getInput() {
    return input;
  }

  /**
   * A method to create instance of user defined class.
   *
   * @param funClass
   * @return instance of supplied class of type Object.
   * @throws PipelineDefinitionException is there is any exception during the instantiation of the
   *     supplied class.
   */
  protected Object createInstance() throws PipelineDefinitionException {
    return Utilities.createInstance(funClass);
  }
}
