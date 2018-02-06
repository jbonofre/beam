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

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;

/** A kafka consumer property builder class. */
public final class KafkaConsumerConfiguration extends KafkaCommonConfiguration
    implements IOConfiguration.KafkaConsumerConfiguration {

  private Pair<Class<? extends Deserializer<?>>, Class<? extends Deserializer<?>>>
      kafkaConsumerSerdes;

  /**
   * @param properties
   * @param decodedURI
   */
  private KafkaConsumerConfiguration(Map<String, String> properties, BeamUri decodedURI) {
    super(properties, decodedURI);
    kafkaConsumerSerdes =
        KafkaDefaultSerdes.getDeserializersIfAvailable(
            getValueDeserializer(), getKeyDeserializer());
  }

  /** A utility method to check the key serializer is defined. */
  public boolean isKeyDeserializerDefined() {
    return getProperties().get(KEY_DESERIALIZER) != null;
  }
  /** A utility method to check the key serializer is defined. */
  public boolean isValueDeserializerDefined() {
    return getProperties().get(KEY_DESERIALIZER) != null;
  }
  /** Returns kafka Key Deserializer class. */
  public String getKeyDeserializer() {
    return getProperties().get(KEY_DESERIALIZER);
  }
  /** A utility method that returns the key deserializer class. */
  public Class<? extends Deserializer<?>> getKeyDeserializerClass() {
    return kafkaConsumerSerdes.getLeft();
  }

  /** A utility method that returns the value deserializer class. */
  public Class<? extends Deserializer<?>> getValueDeserializerClass() {
    return kafkaConsumerSerdes.getRight();
  }
  /** Returns kafka value deserializer class. */
  public String getValueDeserializer() {
    return getProperties().get(VALUE_DESERIALIZER);
  }
  /**
   * A builder method for this class.
   *
   * @param properties Properties from the {@link InputType}
   * @return instance of this class
   */
  public static KafkaConsumerConfiguration build(
      Map<String, String> properties, BeamUri decodedURI) {
    return new KafkaConsumerConfiguration(properties, decodedURI);
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
    getProperties().remove(KEY_DESERIALIZER);
    getProperties().remove(VALUE_DESERIALIZER);
  }
}
