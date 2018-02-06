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
package org.apache.beam.sdk.extensions.dsl.xml;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * An example that reads the data from kafka and writes output to a file.
 *
 * <p>Note: Before running this example, you must setup a kafka broker.
 *
 * <p>To execute this pipeline locally, please use the syntax below:
 *
 * <pre>{@code
 * --o=<Absolute path of your output file or directory>
 * --t=< A kafka topic name>
 * --b=< kafka broker address>
 * }</pre>
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *See examples/java/README.md for instructions about how to configure different runners.
 */
public class KafkaExamples {

  /** Inherits standard configuration options. */
  private interface Options extends PipelineOptions {
    @Description("A comma separated list of kafka brokers")
    @Default.String("localhost:9092")
    String getB();

    void setB(String value);

    @Description("A kafka topic name.")
    @Default.String("test")
    String getT();

    void setT(String value);

    @Description("Absolute path of an output file.")
    @Default.String("/tmp/output/")
    String getO();

    void setO(String value);
  }

  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    p.apply(
            KafkaIO.<String, String>read()
                .withBootstrapServers(options.getB())
                .withTopic(options.getT())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class))
        .apply(ParDo.of(new BasicTransformer()))
        .apply(new WindowRunner())
        .apply(TextIO.write().withWindowedWrites().withNumShards(1).to(options.getO()));

    p.run().waitUntilFinish();
  }
}
