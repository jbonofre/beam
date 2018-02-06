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

package org.apache.beam.sdk.extensions.dsls.xml.flow.loader;

import java.io.File;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.dsls.xml.flow.loader.exception.FlowDescriptorValidationException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.exception.PipelineDefinitionException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.FlowDefinitionType;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsFactory.Builder;

/** Main runner class for the XML DSL. */
public class BeamFlowDSLRunner {

  public static void main(final String[] args)
      throws FlowDescriptorValidationException, PipelineDefinitionException {
    PipelineOptionsFactory.register(FlowXmlOptions.class);
    FlowXmlOptions flow =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(FlowXmlOptions.class);
    Builder pipelineBuilder = PipelineOptionsFactory.fromArgs(args);

    Pipeline pipeline = Pipeline.create(pipelineBuilder.withValidation().create());
    final String flowDefinition = flow.getDslXml();
    FlowDefinitionType currentFlowDefinition =
        new FlowUnmashaller().getFlowDefinition(new File(flowDefinition));
    PipelineMaker.builder()
        .registerFlowDefinition(currentFlowDefinition)
        .attachePipeline(pipeline)
        .build()
        .applyFlow()
        .run()
        .waitUntilFinish();
  }
}
