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


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.HashMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;



/**
 * This class loads few predefined kafka serializer and deserializer classes into a cache. It
 * exposes few methods to query the pre-loaded serdes. This class will support user defined serdes
 * also in future.
 */
final class KafkaDefaultSerdes {
  private static ImmutableMap<String, Class<? extends Deserializer<?>>> deserializerCache = null;
  private static ImmutableMap<String, Class<? extends Serializer<?>>> serializerCache = null;

  static {
    serializerCache = KafkaDefaultSerdes.loadDefaultSerializers();
    deserializerCache = KafkaDefaultSerdes.loadDefaultDeserializers();
  }

  public static Class<? extends Deserializer<?>> getDeserializer(String className) {
    return deserializerCache.get(className);
  }

  public static ImmutableMap<String, Class<? extends Deserializer<?>>> getDeserializerCache() {
    return deserializerCache;
  }
  /**
   * This class creates a pair of kafka key and value Deserializer and returns to the caller a a
   * {@link Pair} object. The left side of the Pair contains the Key class Deserializer and right
   * side contains value Deserializer classes.
   *
   * @param serializerValueClass a string representation kafka {@link Deserializer} class.
   * @param serializerKeyClass a String representation of kafka {@link Deserializer} class.
   * @return instance of {@link Pair}
   */
  public static Pair<Class<? extends Deserializer<?>>, Class<? extends Deserializer<?>>>
      getDeserializersIfAvailable(String deserValueClass, String deserKeyClass) {
    Class<? extends Deserializer<?>> valClass = getDeserializer(deserValueClass);
    Class<? extends Deserializer<?>> keyClass = getDeserializer(deserKeyClass);
    if (valClass == null) {
      // need fix for the non default Deserializers
      // Class<?> forName = Class.forName(keyDeserializer);
    }
    if (keyClass == null) {}

    Pair<Class<? extends Deserializer<?>>, Class<? extends Deserializer<?>>> of =
        Pair.<Class<? extends Deserializer<?>>, Class<? extends Deserializer<?>>>of(
            keyClass, valClass);
    return of;
  }

  /**
   * This class creates a pair of kafka key and value Serializers and returns to the caller a a
   * {@link Pair} object. The left side of the Pair contains the Key class serializer and right side
   * contains value serializer classes.
   *
   * @param serializerValueClass a string representation kafka {@link Serializer} class.
   * @param serializerKeyClass a String representation of kafka {@link Serializer} class.
   * @return instance of {@link Pair}
   */
  public static Pair<Class<? extends Serializer<?>>, Class<? extends Serializer<?>>>
      getSerializersIfAvailable(String serializerValueClass, String serializerKeyClass) {
    Class<? extends Serializer<?>> valClass = getSerializer(serializerValueClass);
    Class<? extends Serializer<?>> keyClass = getSerializer(serializerKeyClass);
    if (valClass == null) {
      // need fix for the non default Deserializers
      // Class<?> forName = Class.forName(keyDeserializer);
    }
    if (keyClass == null) {}

    Pair<Class<? extends Serializer<?>>, Class<? extends Serializer<?>>> of =
        Pair.<Class<? extends Serializer<?>>, Class<? extends Serializer<?>>>of(keyClass, valClass);
    return of;
  }

  public static Class<? extends Serializer<?>> getSerializer(String className) {
    return serializerCache.get(className);
  }

  public static ImmutableMap<String, Class<? extends Serializer<?>>> getSerializerCache() {
    return serializerCache;
  }

  public static ImmutableMap<String, Class<? extends Deserializer<?>>> loadDefaultDeserializers() {

    HashMap<String, Class<? extends Deserializer<?>>> mapping =
        Maps.<String, Class<? extends Deserializer<?>>>newHashMap();
    mapping.put(
        "org.apache.kafka.common.serialization.ByteArrayDeserializer", ByteArrayDeserializer.class);
    mapping.put(
        "org.apache.kafka.common.serialization.StringDeserializer", StringDeserializer.class);
    mapping.put("org.apache.kafka.common.serialization.BytesDeserializer", BytesDeserializer.class);
    mapping.put("org.apache.kafka.common.serialization.LongDeserializer", LongDeserializer.class);
    mapping.put(
        "org.apache.kafka.common.serialization.ByteBufferDeserializer",
        ByteBufferDeserializer.class);
    mapping.put(
        "org.apache.kafka.common.serialization.IntegerDeserializer", IntegerDeserializer.class);
    mapping.put("org.apache.kafka.common.serialization.DoubleSerializer", DoubleDeserializer.class);

    return ImmutableMap.<String, Class<? extends Deserializer<?>>>copyOf(mapping);
  }

  public static ImmutableMap<String, Class<? extends Serializer<?>>> loadDefaultSerializers() {

    HashMap<String, Class<? extends Serializer<?>>> mapping =
        Maps.<String, Class<? extends Serializer<?>>>newHashMap();
    mapping.put(
        "org.apache.kafka.common.serialization.ByteArraySerializer", ByteArraySerializer.class);
    mapping.put("org.apache.kafka.common.serialization.DoubleSerializer", DoubleSerializer.class);
    mapping.put("org.apache.kafka.common.serialization.IntegerSerializer", IntegerSerializer.class);
    mapping.put("org.apache.kafka.common.serialization.StringSerializer", StringSerializer.class);
    mapping.put(
        "org.apache.kafka.common.serialization.ByteBufferSerializer", ByteBufferSerializer.class);
    mapping.put("org.apache.kafka.common.serialization.LongSerializer", LongSerializer.class);
    mapping.put("org.apache.kafka.common.serialization.BytesSerializer", BytesSerializer.class);

    return ImmutableMap.<String, Class<? extends Serializer<?>>>copyOf(mapping);
  }
}
