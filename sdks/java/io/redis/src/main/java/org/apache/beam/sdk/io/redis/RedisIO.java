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
package org.apache.beam.sdk.io.redis;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

/**
 * An IO to manipulate Redis key/value database.
 *
 * <h3>Reading Redis key/value pairs</h3>
 *
 * <p>{@link #read()} provides a source which returns a bounded {@link PCollection} containing
 * key/value pairs as {@code KV<String, String>}.
 *
 * <p>To configure a Redis source, you have to provide Redis server hostname and port number.
 * Optionally, you can provide a key pattern (to filter the keys). The following example
 * illustrates how to configure a source:
 *
 * <pre>{@code
 *
 *  pipeline.apply(RedisIO.read()
 *    .withConnectionConfiguration(
 *      RedisConnectionConfiguration.create().withHost("localhost").withPort(6379))
 *    .withKeyPattern("foo*")
 *
 * }</pre>
 *
 * <p>It's also possible to specify host and port directly in the create method of the
 * RedisConnectionConfiguration:
 *
 * <pre>{@code
 *
 *  pipeline.apply(RedisIO.read()
 *    .withConnectionConfiguration(RedisConnectionConfiguration.create("localhost", 6379))
 *    .withKeyPattern("foo*")
 *
 * }</pre>
 *
 * <p>{@link #readAll()} can be used to request Redis server using input PCollection elements as key
 * pattern (as String).
 *
 * <pre>{@code
 *
 *  pipeline.apply(...)
 *     // here we have a PCollection<String> with the key patterns
 *     .apply(RedisIO.readAll().withConnectionConfiguration(
 *      RedisConnectionConfiguration.create().withHost("localhost").withPort(6379))
 *    // here we have a PCollection<KV<String,String>>
 *
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class RedisIO {

  private static final Logger LOG = LoggerFactory.getLogger(RedisIO.class);

  /**
   * Read data from a Redis server.
   */
  public static Read read() {
    return new AutoValue_RedisIO_Read.Builder().setKeyPattern("*").build();
  }

  /**
   * Like {@link #read()} but executes multiple instances of the Redis query substituting each
   * element of a {@link PCollection} as key pattern.
   */
  public static ReadAll readAll() {
    return new AutoValue_RedisIO_ReadAll.Builder().build();
  }

  private RedisIO() {
  }

  /**
   * Implementation of {@link #read()}.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KV<String, String>>> {

    @Nullable abstract RedisConnectionConfiguration connectionConfiguration();
    @Nullable abstract String keyPattern();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      @Nullable abstract Builder setConnectionConfiguration(
          RedisConnectionConfiguration connection);
      @Nullable abstract Builder setKeyPattern(String keyPattern);
      abstract Read build();
    }

    public Read withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "connection can not be null");
      return builder().setConnectionConfiguration(connection).build();
    }

    public Read withKeyPattern(String keyPattern) {
      checkArgument(keyPattern != null, "keyPattern can not be null");
      return builder().setKeyPattern(keyPattern).build();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      connectionConfiguration().populateDisplayData(builder);
    }

    @Override
    public PCollection<KV<String, String>> expand(PBegin input) {
      checkArgument(connectionConfiguration() != null,
          "withConnectionConfiguration() is required");

      return input
          .apply(Create.of(keyPattern()))
          .apply(RedisIO.readAll().withConnectionConfiguration(connectionConfiguration()));
    }

  }

  /**
   * Implementation of {@link #readAll()}.
   */
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {

    @Nullable abstract RedisConnectionConfiguration connectionConfiguration();

    abstract ReadAll.Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      @Nullable abstract ReadAll.Builder setConnectionConfiguration(
          RedisConnectionConfiguration connection);
      abstract ReadAll build();
    }

    public ReadAll withConnectionConfiguration(RedisConnectionConfiguration connection) {
      checkArgument(connection != null, "connection can not be null");
      return builder().setConnectionConfiguration(connection).build();
    }

    @Override
    public PCollection<KV<String, String>> expand(PCollection<String> input) {
      checkArgument(connectionConfiguration() != null,
          "withConnectionConfiguration() is required");

      return input.apply(ParDo.of(new ReadFn(connectionConfiguration())))
          .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
          .apply(new Reparallelize());
    }

  }

  /**
   * A {@link DoFn} requesting Redis server to get key/value pairs.
   */
  private static class ReadFn extends DoFn<String, KV<String, String>> {

    private final RedisConnectionConfiguration connectionConfiguration;

    private transient Jedis jedis;

    public ReadFn(RedisConnectionConfiguration connectionConfiguration) {
      this.connectionConfiguration = connectionConfiguration;
    }

    @Setup
    public void setup() {
      jedis = connectionConfiguration.connect();
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      ScanParams scanParams = new ScanParams();
      scanParams.match(processContext.element());

      String cursor = ScanParams.SCAN_POINTER_START;
      boolean finished = false;
      while (!finished) {
        ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
        List<String> keys = scanResult.getResult();

        Pipeline pipeline = jedis.pipelined();
        if (keys != null) {
          for (String key : keys) {
            pipeline.get(key);
          }
          List<Object> values = pipeline.syncAndReturnAll();
          for (int i = 0; i < values.size(); i++) {
            processContext.output(KV.of(keys.get(i), (String) values.get(i)));
          }
        }

        cursor = scanResult.getStringCursor();
        if (cursor.equals("0")) {
          finished = true;
        }
      }
    }

    @Teardown
    public void teardown() {
      jedis.close();
    }

  }

  private static class Reparallelize
      extends PTransform<PCollection<KV<String, String>>, PCollection<KV<String, String>>> {

    @Override public PCollection<KV<String, String>> expand(PCollection<KV<String, String>> input) {
      // reparallelize mimics the same behavior as in JdbcIO
      // breaking fusion
      PCollectionView<Iterable<KV<String, String>>> empty = input
          .apply("Consume",
              Filter.by(SerializableFunctions.<KV<String, String>, Boolean>constant(false)))
          .apply(View.<KV<String, String>>asIterable());
      PCollection<KV<String, String>> materialized = input
          .apply("Identity", ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
              context.output(context.element());
            }
      }).withSideInputs(empty));
      return materialized
          .apply("Pair with random key", ParDo.of(
              new DoFn<KV<String, String>, KV<Integer, KV<String, String>>>() {
                private int shard;

                @Setup
                public void setup() {
                  shard = ThreadLocalRandom.current().nextInt();
                }

                @ProcessElement
                public void processElement(ProcessContext context) {
                  context.output(KV.of(++shard, context.element()));
                }
              }
          ))
          .apply(Reshuffle.<Integer, KV<String, String>>of())
          .apply(Values.<KV<String, String>>create());
    }
  }

}
