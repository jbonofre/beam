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

package org.apache.beam.sdk.extensions.dsls.xml.example.wordcount;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
 * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to a
 * ParDo in the pipeline.
 */
public class ExtractWordsFn extends DoFn<String, String> {
  /*
   * Generated serial version id.
   */
  private static final long serialVersionUID = -2042783568653893050L;
  private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
  public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

  @ProcessElement
  public void processElement(ProcessContext c) {
    if (c.element().trim().isEmpty()) {
      emptyLines.inc();
    }

    // Split the line into words.
    String[] words = c.element().split(TOKENIZER_PATTERN);

    // Output each word encountered into the output PCollection.
    for (String word : words) {
      if (!word.isEmpty()) {
        c.output(word);
      }
    }
  }
}
