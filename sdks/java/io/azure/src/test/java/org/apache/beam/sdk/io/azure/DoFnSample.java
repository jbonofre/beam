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
package org.apache.beam.sdk.io.azure;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * Public class, so javadoc is a must have.
 */
public class DoFnSample {

  /**
   * Public method, so javadoc is a must have.
   *
   * @param args Main args.
   */
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    Pipeline pipeline = Pipeline.create(options);
    List<String> data = new ArrayList();
    data.add("filename");
    pipeline.apply(Create.of(data))
        .apply("Read from Azure", ParDo.of(new DoFn<String, String>() {

          // AzureClient

          @Setup
          public void setup() throws Exception {
            // init of the azure client
          }

          @ProcessElement
          public void processElement(ProcessContext processContext) throws Exception {
            String filename = processContext.element();
            // read the file at filename using the azure client
            // for instance if we have multiple file
            try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
              String line;
              while ((line = reader.readLine()) != null) {
                processContext.output(line);
              }
            }
          }

          @Teardown
          public void teardown() throws Exception {
            // stop/shutdown of the azure client
          }

        }))
        .apply("Display Lines", ParDo.of(new DoFn<String, Void>() {
          public void processElement(ProcessContext processContext) throws Exception {
            String element = processContext.element();
            System.out.println(element);
          }
        }));
  }

}
