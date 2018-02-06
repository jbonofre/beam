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
package org.apache.beam.sdk.extensions.dsls.xml.flow.io.kafka.config;

import java.util.Map;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration.KafkaProducerConfiguraton;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serializer;

/** A kafka producer property handler class. */
public final class KafkaProducerConfiguration extends KafkaCommonConfiguration
    implements KafkaProducerConfiguraton {
  private Pair<Class<? extends Serializer<?>>, Class<? extends Serializer<?>>> kafkaProducerSerdes;

  /** A private constructor for this class. */
  private KafkaProducerConfiguration(Map<String, String> properties, BeamUri uri) {
    super(properties, uri);
    kafkaProducerSerdes =
        KafkaDefaultSerdes.getSerializersIfAvailable(getValueSerializer(), getKeySerializer());
  }

  /** Returns kafka Key deserializer class. */
  public String getKeySerializer() {
    return getProperties().get(KEY_SERIALIZER);
  }

  /** Returns kafka Key deserializer class. */
  public boolean isKeySerializerDefined() {
    return getProperties().get(KEY_SERIALIZER) != null;
  }
  /** Returns kafka value deserializer class. */
  public String getValueSerializer() {
    return getProperties().get(VALUE_SERIALIZER);
  }

  /** Returns kafka value deserializer class. */
  public boolean isValueSerializerDefined() {
    return getProperties().get(VALUE_SERIALIZER) != null;
  }
  /** A utility method that returns the key deserializer class. */
  public Class<? extends Serializer<?>> getKeySerializerClass() {
    return kafkaProducerSerdes.getLeft();
  }

  /** A utility method that returns the value deserializer class. */
  public Class<? extends Serializer<?>> getValueSerializerClass() {
    return kafkaProducerSerdes.getRight();
  }
  /**
   * A utility method for constructing this class. This method returns an instance of this class.
   *
   * @param properties Properties from the {@link InputType}
   * @return instance of this class
   */
  public static KafkaProducerConfiguration build(Map<String, String> properties, BeamUri uri) {
    return new KafkaProducerConfiguration(properties, uri);
  }

  public long getMaxNumRecords() {
    String maxPollRecords = getProperties().get(MAX_POLL_RECORDS);
    if (maxPollRecords != null && !maxPollRecords.isEmpty()) {
      return Long.parseLong(maxPollRecords);
    }
    return 0;
  }

  @Override
  protected void removeClientTypeSpecificConfiguruation() {
   getProperties().remove(KEY_SERIALIZER);
   getProperties().remove(VALUE_SERIALIZER);
  }
}
