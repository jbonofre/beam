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
package org.apache.beam.runners.spark.io.hadoop;

import java.io.IOException;

import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.hdfs.HDFSFileSink;
import org.apache.beam.sdk.io.hdfs.WritableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/**
 * Test BEAM-1206 (consequence of BEAM-649).
 */
public class WritingSinkTest {

  @Test
  public void testHdfsSink() throws IOException {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    String fileSpec = fs.getUri().resolve("/tmp/test/input.csv").toString();

    fs.delete(new Path(fileSpec), true);

    SparkConf conf = new SparkConf();
    conf.setAppName("testBeamWrite");
    conf.setMaster("local");
    conf.set("spark.hadoop." + CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, "file:///tmp/localfs");
    SparkContextOptions options = PipelineOptionsFactory.as(SparkContextOptions.class);
    options.setRunner(SparkRunner.class);
    options.setUsesProvidedSparkContext(true);
    options.setProvidedSparkContext(new JavaSparkContext(conf));
    final Pipeline p = Pipeline.create(options);

    PCollection<KV<NullWritable, Text>> input = p.apply(
        Create.of(
            KV.of(NullWritable.get(), new Text("one")),
            KV.of(NullWritable.get(), new Text("two")))
            .withCoder(KvCoder.of(WritableCoder.of(NullWritable.class),
                WritableCoder.of(Text.class))));
    input.apply(Write.to(new HDFSFileSink(fileSpec, TextOutputFormat.class, new Configuration())));

    p.run().waitUntilFinish();
  }

}
