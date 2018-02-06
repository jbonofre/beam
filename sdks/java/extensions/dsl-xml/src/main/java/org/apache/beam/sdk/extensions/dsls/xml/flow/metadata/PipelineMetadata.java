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

package org.apache.beam.sdk.extensions.dsls.xml.flow.metadata;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputsType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputsType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.PipelineType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.ProcessingStepsType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.StepType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities;

/**
 * The {@link PipelineMetadata} class constructs the data flow DAG from the flow DSL definitions.
 */
public final class PipelineMetadata implements Serializable {

  /** Generated Serial Version ID. */
  private static final long serialVersionUID = -6266261888360968261L;

  private final PipelineType pipelineType;
  private final Map<String, InputType> inputs;
  private final Map<String, OutputType> outputs;
  private final Map<String, StepType> steps;
  private final Map<String, InputType> inputsMappedByUri;
  private final Map<String, OutputType> outputsMappedByUri;

  private PipelineMetadata(Builder builder) {
    this.pipelineType = builder.pipelineType;
    this.inputs = builder.inputs;
    this.outputs = builder.outputs;
    this.steps = builder.steps;
    this.inputsMappedByUri = builder.inputsMappedByUri;
    this.outputsMappedByUri = builder.outputsMappedByUri;
  }

  private PipelineMetadata(
      @Nonnull Map<String, InputType> inputs,
      @Nonnull Map<String, StepType> steps,
      @Nonnull Map<String, OutputType> outputs,
      @Nonnull Map<String, InputType> inputsMappedByUri,
      @Nonnull Map<String, OutputType> outputsMappedByUri,
      @Nonnull PipelineType pipelineTypeData) {
    this.inputs = inputs;
    this.outputs = outputs;
    this.steps = steps;
    this.inputsMappedByUri = inputsMappedByUri;
    this.outputsMappedByUri = outputsMappedByUri;
    this.pipelineType = pipelineTypeData;
  }

  public PipelineType getPipelineMetadata() {
    return pipelineType;
  }

  public InputType getInput(@Nonnull String name) {
    validateArguments(name, "Input");
    InputType inputType = inputs.get(name);
    checkState(inputType != null, name + " type input does not exist in the Flow XML file.");
    return inputType;
  }

  private void validateArguments(@Nonnull String name, @Nonnull String type) {
    checkArgument(name != null, type + " name " + name + " argument cannot be null ");
    checkArgument(
        (name.length() != 0), type + " name " + name + " argument cannot be null or empty.");
  }

  public OutputType getOutputs(@Nonnull String name) {
    validateArguments(name, "Output");
    OutputType outputType = outputs.get(name);
    checkState(outputType != null, name + " type output does not exist in the Flow XML file.");
    return outputType;
  }

  public InputType getInputByUri(@Nonnull String uriScheme) {
    validateArguments(uriScheme, "Input");
    InputType inputType = inputsMappedByUri.get(uriScheme);
    checkState(inputType != null, uriScheme + " type Input does not exist in the Flow XML file.");
    return inputType;
  }

  public OutputType getOutputByUri(@Nonnull String uriScheme) {
    validateArguments(uriScheme, "Output");
    OutputType outputType = outputsMappedByUri.get(uriScheme);
    checkState(outputType != null, uriScheme + " type output does not exist in the Flow XML file.");
    return outputType;
  }

  public StepType getStep(@Nonnull String name) {
    validateArguments(name, "Step");
    StepType stepType = steps.get(name.toLowerCase());
    // checkState(stepType != null, name + " type Step does not exist in the Flow XML file.");
    return stepType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((inputs == null) ? 0 : inputs.hashCode());
    result = prime * result + ((outputs == null) ? 0 : outputs.hashCode());
    result = prime * result + ((steps == null) ? 0 : steps.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PipelineMetadata other = (PipelineMetadata) obj;
    if (inputs == null) {
      if (other.inputs != null) {
        return false;
      }
    } else if (!inputs.equals(other.inputs)) {
      return false;
    }
    if (outputs == null) {
      if (other.outputs != null) {
        return false;
      }
    } else if (!outputs.equals(other.outputs)) {
      return false;
    }
    if (steps == null) {
      if (other.steps != null) {
        return false;
      }
    } else if (!steps.equals(other.steps)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "PipelineMetadata [inputs="
        + convertToString("Inputs", inputs.keySet())
        + ", outputs="
        + convertToString("Outputs", outputs.keySet())
        + ", steps="
        + convertToString("Steps", steps.keySet())
        + "]";
  }

  private String convertToString(String name, Set<String> keySet) {
    StringBuffer buffer = new StringBuffer();
    buffer.append(name + " -> ");
    buffer.append(keySet);
    return buffer.toString();
  }

  /**
   * Creates builder to build {@link PipelineMetadata}.
   *
   * @return created builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder to build {@link PipelineMetadata}. */
  public static final class Builder {

    private PipelineType pipelineType;
    private Map<String, InputType> inputs;
    private Map<String, OutputType> outputs;
    private Map<String, StepType> steps;
    private Map<String, InputType> inputsMappedByUri;
    private Map<String, OutputType> outputsMappedByUri;

    private Builder() {}

    public Builder withPipelineType(PipelineType pipelineType) {
      this.pipelineType = pipelineType;
      return this;
    }

    public Builder withInputs(InputsType inputs) {
      this.inputs = Utilities.mapInputsByName(inputs.getInput());
      this.inputsMappedByUri = Utilities.mapInputsByUriScheme(inputs.getInput());
      return this;
    }

    public Builder withOutputs(OutputsType outputs) {
      this.outputs = Utilities.mapOutputByName(outputs.getOutput());
      this.outputsMappedByUri = Utilities.mapOutputsByUriScheme(outputs.getOutput());
      return this;
    }

    public Builder withSteps(ProcessingStepsType steps) {
      this.steps = Utilities.mapStepsByName(steps.getStep());
      return this;
    }

    public Builder withInputsMappedByUri(Map<String, InputType> inputsMappedByUri) {
      this.inputsMappedByUri = inputsMappedByUri;
      return this;
    }

    public Builder withOutputsMappedByUri(Map<String, OutputType> outputsMappedByUri) {
      this.outputsMappedByUri = outputsMappedByUri;
      return this;
    }

    public PipelineMetadata build() {
      return new PipelineMetadata(this);
    }
  }
}
