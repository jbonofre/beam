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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.dsls.xml.flow.handlers.DslStepsHandlerFactory;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.IOAdapterFactory;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.PipelineMetadata;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.exception.PipelineDefinitionException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.ApplyType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.FlowDefinitionType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.PipelineType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.StepType;
import org.apache.beam.sdk.values.PCollection;

/**
 * This class is responsible for creating Apache Beam data pipelines via declarative API defined in
 * XML format. It accepts the transformed XML pipeline metadata and converts into Apache beam data
 * processing entities such as transform operations, source and sink connectors. After this
 * translation it chain the different operations into a beam data flow.
 */
public class PipelineMaker {

  /*
   *  A reference to Flow DSL metadata.
   */
  private PipelineMetadata metadata;
  /*
   * A reference to Apache beam {@link Pipeline}.
   */
  private Pipeline pipeline;

  /**
   * A private constructor of this class.
   * @param metadata is the flow DSL metadata.
   * @param pipeline,is a beam {@link Pipeline} reference.
   */
  private PipelineMaker(PipelineMetadata metadata, Pipeline pipeline) {
    this.pipeline = pipeline;
    this.metadata = metadata;
  }

  public Pipeline applyFlow() throws PipelineDefinitionException {
    PipelineType pipelineMetadata = metadata.getPipelineMetadata();
    PCollection<?> beginWith =
        IOAdapterFactory.createInputAdapterAndApply(
            pipeline, metadata.getInput(pipelineMetadata.getFrom().getStreams()));

    List<ApplyType> to = pipelineMetadata.getTo();
    for (ApplyType applyType : to) {
      StepType type = metadata.getStep(applyType.getApply());
      if (type == null) {
        OutputType outputs = metadata.getOutputs(applyType.getApply());
        IOAdapterFactory.createOutputAdapterAndApply(outputs, beginWith);
        break;
      }
      beginWith = DslStepsHandlerFactory.createAndApply(type, beginWith);
    }
    return pipeline;
  }

  /* Creates builder to build {@link PipelineMaker}.
   *
   * @return created builder
   */
  public static Builder builder() {
    return new Builder();
  }
  /** Builder to build {@link PipelineMaker}. */
  public static final class Builder {
    private FlowDefinitionType currentFlowDefinition;
    private Pipeline pipeline;

    /**
     * No need to create this builder externally.
     */
    private Builder() {}

    /**Apply the flow metadata for further processing.
     */
    public Builder registerFlowDefinition(FlowDefinitionType currentFlowDefinition) {
      checkArgument(currentFlowDefinition != null, " Flow definition cannot be null");
      this.currentFlowDefinition = currentFlowDefinition;
      return this;
    }

    /**
     * This method register data pipeline.
     */
    public Builder attachePipeline(Pipeline pipeline) {
      checkArgument(pipeline != null, " Beam data pipeline  cannot be null");
      this.pipeline = pipeline;
      return this;
    }

    public PipelineMaker build() {
      checkState(currentFlowDefinition != null, "Beam data flow dsl  is not configured correctly");
      checkState(pipeline != null, "Beam pipeline is not configured correctly");
      PipelineMetadata metadata =
          PipelineMetadata.builder()
              .withInputs(currentFlowDefinition.getInputs())
              .withPipelineType(currentFlowDefinition.getPipeline())
              .withOutputs(currentFlowDefinition.getOutputs())
              .withSteps(currentFlowDefinition.getSteps())
              .build();
      return new PipelineMaker(metadata, pipeline);
    }
  }
}
