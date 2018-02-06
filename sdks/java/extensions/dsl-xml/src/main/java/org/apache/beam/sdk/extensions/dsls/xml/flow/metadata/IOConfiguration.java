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
package org.apache.beam.sdk.extensions.dsls.xml.flow.metadata;

import java.io.Serializable;

/** Beam DSL configuration names are defined in this class. */
public interface IOConfiguration extends Serializable {

  /** A configuration for the File IO adapter. */
  interface FileIOConfiguration extends Serializable {
    /** File header for the for the output files. */

    /** absolute path of the input or out file/directory. */
    String FILE_PATH = "file.path";

    /** Mapped name for the File IO Adapter. */
    String SCHEME_TYPE = "file";
    /** Scheme for google file system. */
    String GS_SCHEME_TYPE = "gs";
  }

  /**
   * A configuration for File input operation.
   *
   */
  interface FileInputConfiguration extends FileIOConfiguration {
    /** File compression types. */
    String COMPRESSION_TYPE = "compression.type";
  }

  /**
   *A configuration constants for the File output operation.
   *
   */
  interface FileOutputConfiguration extends FileIOConfiguration {
    String HEADER = "file.header";

    /** Footer for the output files. */
    String FOOTER = "file.footer";
    /** Number of shards for the output files. */
    String NUM_SHARDS = "num.shards";
    /** Shard file name template. */
    String SHARD_FILE_NAME_TEMPLATE = "file.shards.name.template";
    /** Suffix for the output file. */
    String SUFFIX = "file.suffix";
    /** Windowed writes enabled or not. The possible values are tru or false */
    String WINDOWED_WRITES = "file.windowed.writes";
  }
  /** A configuration for the File IO adapter. */
  interface BigQueryIO {
    /** Mapped name for the File IO Adapter. */
    String SCHEME_TYPE = "bq";
  }

  /** A configuration for the File IO adapter. */
  interface JdbcIO {
    /** Mapped name for the File IO Adapter. */
    String SCHEME_TYPE = "jdbc";
  }

  /** All Kafka specific mandatory common configurations defined as constants in this interface. */
  interface KafkaCommonConfiguaration {

    String SCHEME_TYPE = "kafka";
    /**
     * <code> part of URI or as a property kafka.topic</code>
     *
     * <p>One or comma separated kafka topic names.
     */
    String TOPICS = "kafka.topics";
    /**
     * A list of host/port pairs to use for establishing the initial connection to the Kafka
     * cluster.
     */
    String BOOTSRTAP_SERVERS = "bootstrap.servers";
  }
  /**
   * This interface defines a set of constants that represents the Apache Kafka Consumer
   * configuration properties.
   */
  interface KafkaConsumerConfiguration extends KafkaCommonConfiguaration {
    /**
     * <code>key.deserializer</code>
     *
     * <p>Deserializer class for key that implements the <code>Deserializer</code> interface.
     */
    String KEY_DESERIALIZER = "key.deserializer";
    /**
     * <code>value.deserializer</code>.
     *
     * <p>Deserializer class for value that implements the <code>Deserializer</code> interface."
     */
    String VALUE_DESERIALIZER = "value.deserializer";
    /**
     * <code>group.id</code>
     *
     * <p>A unique string that identifies the consumer group this consumer belongs to.
     */
    String GROUP_ID_CONFIG = "group.id";

    String MAX_POLL_RECORDS = "max.poll.records";
  }
  /**
   * This interface defines a set of constants that represents the Apache Kafka Consumer
   * configuration properties.
   */
  interface KafkaProducerConfiguraton extends KafkaCommonConfiguaration {
    /**
     * <code>key.deserializer</code>
     *
     * <p>Deserializer class for key that implements the <code>Deserializer</code> interface.
     */
    String KEY_SERIALIZER = "key.serializer";
    /**
     * <code>value.deserializer</code>.
     *
     * <p>Deserializer class for value that implements the <code>Deserializer</code> interface."
     */
    String VALUE_SERIALIZER = "value.serializer";

    String MAX_POLL_RECORDS = "max.poll.records";

    /**
     * <code>publish.value.only</code> If this flag enabled in the flow xml property for the kafka
     * output, it will publishe only values. POssible values are true or false and default value is
     * false;
     */
    String VALUE_ONLY = "publish.value.only";
  }
}
