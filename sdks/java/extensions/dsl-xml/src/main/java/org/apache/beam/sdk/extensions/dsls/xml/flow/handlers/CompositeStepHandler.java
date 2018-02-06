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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * A concrete implementation for Composite type Functions. This is a custom composite transform that
 * bundles more than one transforms as a reusable PTransform subclass. Using composite transforms
 * allows for easy reuse, modular testing, and an improved monitoring experience.
 */
public class CompositeStepHandler<T, V> extends AbstractStepHandler<T, V> {

  /** Generated Serial Version ID. */
  private static final long serialVersionUID = -4660017432493403419L;

  public CompositeStepHandler<T, V> assign(StepType metadata, PCollection<T> input) {
    super.assign(metadata, input);
    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public PCollection<V> apply() throws PipelineDefinitionException {
    boolean assignableFrom =
        getFunClass()
            .getGenericSuperclass()
            .getTypeName()
            .startsWith("org.apache.beam.sdk.transforms.PTransform");
    if (!assignableFrom) {
      throw new PipelineDefinitionException(
          "The handler class "
              + getFunClass().getName()
              + "defined in the flow xml file is not valid beam DoFn type.");
    }
    @SuppressWarnings("rawtypes")
    final PTransform newInstance = (PTransform) createInstance();
    return (PCollection<V>) getInput().apply(newInstance);
  }

  @Override
  public boolean matched(SupportedType type) {
    return type.equals(SupportedType.COMPOSITE);
  }
}
