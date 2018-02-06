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

package org.apache.beam.sdk.extensions.dsls.xml.flow.io.kafka;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.AbstractIOAdapter;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.IOAdapter;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.kafka.config.KafkaConsumerConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.kafka.config.KafkaProducerConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration.KafkaCommonConfiguaration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaIO.Read;
import org.apache.beam.sdk.io.kafka.KafkaIO.Write;
import org.apache.beam.sdk.values.PCollection;


/**
 * A <a href="http://kafka.apache.org/">Kafka</a> IO adapter for unbounded data stream source and
 * sink. This IO Adapter class uses Beam {@link KafkaIO} component.
 */
@AutoService(IOAdapter.class)
public class KafkaIOAdapter extends AbstractIOAdapter {

  /** Generated Serial version id. */
  private static final long serialVersionUID = -2575117153098957592L;

  private KafkaConsumerConfiguration consumerConfiguration;

  private KafkaProducerConfiguration producerConfiguration;

  @SuppressWarnings("rawtypes")
  private Read getBasicReader() {
    return KafkaIO.read()
        .withTopics(consumerConfiguration.getTopics())
        .withBootstrapServers(consumerConfiguration.getKafkaBrokers())
        .updateConsumerProperties(consumerConfiguration.getDelta());
  }

  private Write<Object, Object> getBasicWriter() {
    return KafkaIO.write()
        .withBootstrapServers(producerConfiguration.getKafkaBrokers())
        .withTopic(producerConfiguration.getTopic());
  }

  /*
   * (non-Javadoc)
   * @see org.apache.beam.sdk.extensions.dsls.xml.flow.io.IOAdapter#getHandlerSchema()
   */
  @Override
  public String getHandlerScheme() {
    return KafkaCommonConfiguaration.SCHEME_TYPE;
  }

  /**
   * A reader method to read data from Kafka topic specified.
   */
  @SuppressWarnings("unchecked")
  public Read<?, ?> read() {

    return getBasicReader()
        .withKeyDeserializer(consumerConfiguration.getKeyDeserializerClass())
        .withValueDeserializer(consumerConfiguration.getValueDeserializerClass());
  }

  public void init(@Nonnull InputType type) {
    super.init(type);
    consumerConfiguration = KafkaConsumerConfiguration.build(getProperties(), getDecodedURI());
  }

  @Override
  public void init(@Nonnull OutputType type) {
    super.init(type);
    producerConfiguration = KafkaProducerConfiguration.build(getProperties(), getDecodedURI());
  }

  public boolean understand(@Nonnull InputType type) {
    return Utilities.getDecodedURI(type.getUri()).getScheme().equalsIgnoreCase(getHandlerScheme());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public Write write() {
    Write kafkaWriter = getBasicWriter();
    if (producerConfiguration.isKeySerializerDefined()) {
      kafkaWriter = kafkaWriter.withKeySerializer(producerConfiguration.getKeySerializerClass());
    }
    if (producerConfiguration.isValueSerializerDefined()) {
      kafkaWriter = kafkaWriter.withValueSerializer(producerConfiguration.getKeySerializerClass());
    }
    kafkaWriter.updateProducerProperties(producerConfiguration.getDelta());
    return kafkaWriter;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public PCollection format(PCollection data) {
    // data format  translation will be done in the next iteration.
    return data;
  }
}
