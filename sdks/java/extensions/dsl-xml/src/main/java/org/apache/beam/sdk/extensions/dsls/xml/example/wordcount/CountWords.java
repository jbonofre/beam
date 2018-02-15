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

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A PTransform that converts a PCollection containing lines of text into a PCollection of formatted
 * word counts.
 *
 * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and Count)
 * as a reusable PTransform subclass. Using composite transforms allows for easy reuse, modular
 * testing, and an improved monitoring experience.
 */
public class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
  /*
   * Generated serial version id.
   */
  private static final long serialVersionUID = -7715215125225677158L;

  @Override
  public PCollection<KV<String, Long>> expand(PCollection<String> words) {

    /*// Convert lines of text into individual words.
    PCollection<String> words = lines.apply(
        ParDo.of(new ExtractWordsFn()));*/

    // Count the number of times each word occurs.
    PCollection<KV<String, Long>> wordCounts = words.apply(Count.<String>perElement());

    return wordCounts;
  }
}
