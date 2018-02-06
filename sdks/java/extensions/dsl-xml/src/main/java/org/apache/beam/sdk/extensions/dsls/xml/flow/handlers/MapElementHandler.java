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
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.SupportedType;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

/** A concrete implementation for ParDo type Functions. */
public class MapElementHandler<T, V> extends AbstractStepHandler<T, V> {

  /** Generated Serial Version ID. */
  private static final long serialVersionUID = -4660017432493403419L;

  @Override
  public MapElementHandler<T, V> assign(StepType metadata, PCollection<T> input) {
    super.assign(metadata, input);
    return this;
  }

  @Override
  public PCollection<V> apply() throws PipelineDefinitionException {

    boolean assignableFrom = SimpleFunction.class.isAssignableFrom(getFunClass());
    if (!assignableFrom) {
      throw new PipelineDefinitionException(
          "The handler class "
              + getFunClass().getName()
              + "defined in the flow xml file is not valid beam DoFn type.");
    }
    @SuppressWarnings("unchecked")
    final SimpleFunction<T, V> newInstance = (SimpleFunction<T, V>) createInstance();
    return getInput().apply(MapElements.<T, V>via(newInstance));
  }

  @Override
  public boolean matched(SupportedType type) {
    return type.equals(SupportedType.MAP);
  }
}
