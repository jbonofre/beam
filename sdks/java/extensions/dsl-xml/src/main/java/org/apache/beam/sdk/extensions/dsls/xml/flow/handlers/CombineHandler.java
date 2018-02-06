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
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.PerKey;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** A concrete implementation for ParDo type Functions. */
public class CombineHandler<K, V> extends AbstractStepHandler<KV<K, V>, V> {
  // SerializableFunction<Iterable<Integer>, Integer>
  /** Generated Serial Version ID. */
  private static final long serialVersionUID = -4660017432493403419L;

  @Override
  public CombineHandler<K, V> assign(StepType metadata, PCollection<KV<K, V>> input) {
    super.assign(metadata, input);

    return this;
  }

  @SuppressWarnings("unchecked")
  @Override
  public PCollection<V> apply() throws PipelineDefinitionException {

    boolean assignableFrom = SerializableFunction.class.isAssignableFrom(getFunClass());
    if (!assignableFrom) {
      throw new PipelineDefinitionException(
          "The handler class "
              + getFunClass().getName()
              + "defined in the flow xml file is not valid beam DoFn type.");
    }
    final SerializableFunction<Iterable<V>, V> newInstance =
        (SerializableFunction<Iterable<V>, V>) createInstance();
    PerKey<K, V, V> perKey = Combine.perKey(newInstance);

    return (PCollection<V>) getInput().apply(perKey);
  }

  @Override
  public boolean matched(SupportedType type) {
    return type.equals(SupportedType.MAP);
  }
}
