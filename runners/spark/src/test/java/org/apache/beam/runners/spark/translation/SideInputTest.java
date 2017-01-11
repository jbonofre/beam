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
package org.apache.beam.runners.spark.translation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests for translation of side inputs in the Spark Runner.
 */
public class SideInputTest {

  @Rule
  public final SparkTestPipelineOptions pipelineOptions = new SparkTestPipelineOptions();

  /**
   * A technique to verify how many times the side input DoFn is called.
   */
  private static final AtomicInteger globalWrites = new AtomicInteger();

  /**
   * Test that a side input is evaluated only once if it is used only once.
   *
   * <p>Not {@link org.apache.beam.sdk.testing.RunnableOnService} since it will not work other
   * than in Spark local mode.
   */
  @Test
  public void testSideInputEvaluatedOnce() {
    // Simple job that tests words against a dictionary to normalize the case.
    Pipeline p = Pipeline.create(pipelineOptions.getOptions());
    PCollection<String> dictionary = p.apply(Create.of("OnE", "TWO", "thrEE"));

    // Prepare the side input.  The function in the DoFn should only be called once per word
    PCollection<KV<String, String>> lcToWord = dictionary.apply(ParDo.of(new ToKVLowerCase()));
    PCollectionView<Map<String, String>> dictView =
        lcToWord.apply(View.<String, String>asMap());

    // Apply the transformation
    PCollection<String> input = p.apply(Create.of("one", "deux", "three"));
    PCollection<String> normalized = input.apply("DetectWords",
        ParDo.of(new ToDictionaryCase(dictView)).withSideInputs(dictView));

    PAssert.that(normalized).containsInAnyOrder("OnE", "thrEE");

    // And run the test
    globalWrites.set(0);
    p.run().waitUntilFinish();
    assertThat(globalWrites.get(), is(3));
  }

  /**
   * For the input word, create a {@link KV} where the key is the lowercase word.
   */
  public static class ToKVLowerCase extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      globalWrites.incrementAndGet();
      c.output(KV.of(c.element().toLowerCase(), c.element()));
    }
  }

  /**
   * Filters words that are not in the dictionary, and returns the word with the same case as
   * the dictionary when present.
   */
  public static class ToDictionaryCase extends DoFn<String, String> {
    private final PCollectionView<Map<String, String>> dictView;

    public ToDictionaryCase(PCollectionView<Map<String, String>> dictView) {
      this.dictView = dictView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String dictionaryCase = c.sideInput(dictView).get(c.element().toLowerCase());
      if (dictionaryCase != null) {
        c.output(dictionaryCase);
      }
    }
  }
}