package org.apache.beam.sdk.io.azure;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class DoFnSample {

  public static void main(String[] args) throws Exception {
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
