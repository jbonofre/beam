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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration.KafkaCommonConfiguaration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities;
import org.apache.commons.lang3.StringUtils;




/**
 * This class contains the common configuration for the Apache Kafka. There are two main know
 * subclass for this class are {@link KafkaProducerConfiguration} and {@link
 * KafkaConsumerConfiguration}
 */
abstract class KafkaCommonConfiguration implements IOConfiguration.KafkaCommonConfiguaration {
 /**
 * User provided beam uri.
 */
private BeamUri kafkaUri;

  /** Kafka input adapter configuration properties. */
  private final Map<String, String> properties;
  /**
   * Default constructor for this class.
   * @param properties the incoming properties of type {@link Map}
   * @param decodedURI the user provided uri.
 */
KafkaCommonConfiguration(Map<String, String> properties, BeamUri decodedURI) {
    this.properties = properties;
    this.kafkaUri = decodedURI;
  }

  public Map<String, Object> getDelta() {
    getProperties().remove(KafkaCommonConfiguaration.BOOTSRTAP_SERVERS);
    getProperties().remove(KafkaCommonConfiguaration.TOPICS);
    removeClientTypeSpecificConfiguruation();
    return Maps.transformValues(
        getProperties(),
        new Function<String, Object>() {
          @Override
          public Object apply(String input) {
            return (Object) input;
          }
        });
  }
  /**
   * This method check the mandatory kafka consumer config property bootstrap servers. if it is not
   * defined IllegalStateException will be thrown.
   *
   * @return the list of Kafka brokers.
   */
  public String getKafkaBrokers() {
    String brokerList = getProperties().get(KafkaCommonConfiguaration.BOOTSRTAP_SERVERS);
    checkState(
        brokerList != null && !brokerList.isEmpty(),
        "Atleast a Kafka broker must be defined for the Kafka consumer.");
    return brokerList;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  /** This method returns comma separated string of topics. */
  public String getTopic() {
    String topic = kafkaUri.getPath();
    checkState(topic != null, "Atleast a Kafka topic must be defined for the Kafka consumer.");
    return topic;
  }

  /** Returns a list of topics. */
  public List<String> getTopics() {
    return Utilities.getTopics(getTopic());
  }

  protected abstract void removeClientTypeSpecificConfiguruation();

  public void setTopic(String topicName) {
    String existingTopics = getProperties().get(KafkaCommonConfiguaration.TOPICS);
    List<String> existingTopicList = null;
    List<String> uriTopicList = null;
    if (existingTopics != null && !existingTopics.isEmpty()) {
      if (existingTopics.contains(",")) {
        existingTopicList = Arrays.asList(existingTopics.split(","));
      }
    }
    if (topicName != null && !topicName.isEmpty()) {
      if (topicName.contains(",")) {
        uriTopicList = Arrays.asList(topicName.split(","));
      }
    }
    if (existingTopicList != null) {
      uriTopicList.addAll(existingTopicList);
      getProperties()
          .put(
              KafkaCommonConfiguaration.TOPICS,
              StringUtils.join(
                  ImmutableSet.copyOf(
                 Iterables.filter(uriTopicList, Predicates.not(Predicates.isNull())))
                      .asList().iterator(),
                  ','));
    } else {
      getProperties().put(KafkaCommonConfiguaration.TOPICS, topicName);
    }
  }
}
