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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.List;

import org.apache.beam.sdk.extensions.dsls.xml.flow.loader.exception.FlowDescriptorValidationException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.ApplyType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.FlowDefinitionType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.FromType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.PipelineType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** A unit test for the FlowUnmarshaller class. */
@RunWith(JUnit4.class)
public class FlowUnmashallerTest {

  private FlowUnmashaller um = null;

  /** @throws java.lang.Exception */
  @Before
  public void setUp() throws Exception {
    um = new FlowUnmashaller();
  }

  /** @throws java.lang.Exception */
  @After
  public void tearDown() throws Exception {
    um = null;
  }

  /**
   * Test method for {@link org.apache.beam.sdk.extensions.dsls.xml.flow.loader.FlowUnmashaller
   * #getFlowDefinition(java.io.File)}.
   *
   * @throws FlowDescriptorValidationException on failure of flow validation.
   */
  @Test
  public void testFlowUnmarshallingCreatesObjects() throws FlowDescriptorValidationException {
    FlowDefinitionType flowDefinition =
        um.getFlowDefinition(new File(Utilities.getFileResource("BeamFlow-File-in-File-out.xml")));
    assertNotNull(flowDefinition);
  }

  /**
   * Test method for {@link org.apache.beam.sdk.extensions.dsls.xml.flow.loader.FlowUnmashaller
   * #getFlowDefinition(java.io.File)}.
   *
   * @throws FlowDescriptorValidationException on failure of flow validation.
   */
  @Test
  public void testFlowStructure() throws FlowDescriptorValidationException {
    FlowDefinitionType flowDefinition =
        um.getFlowDefinition(new File(Utilities.getFileResource("BeamFlow-File-in-File-out.xml")));
    assertNotNull(flowDefinition);
    PipelineType pipeline = flowDefinition.getPipeline();
    assertNotNull(pipeline);
    FromType from = pipeline.getFrom();
    assertTrue(from.getStreams().equalsIgnoreCase("txtInput"));
    List<ApplyType> to = pipeline.getTo();
    assertTrue(to.get(0).getApply().equalsIgnoreCase("ExtractWords"));
    assertTrue(to.get(1).getApply().equalsIgnoreCase("CountWords"));
    assertTrue(to.get(2).getApply().equalsIgnoreCase("MapWords"));
    assertTrue(to.get(3).getApply().equalsIgnoreCase("txtOutput"));
  }

  /**
   * Test method for {@link org.apache.beam.sdk.extensions.dsls.xml.flow.loader.FlowUnmashaller
   * #getFlowDefinition(java.io.File)}.
   *
   * @throws FlowDescriptorValidationException on failure of flow validation.
   */
  @Test
  public void testFlowPipelineStrcutureConsistancy() throws FlowDescriptorValidationException {
    PipelineType p = new PipelineType();
    pipeFiller(p, "one");
    pipeFiller(p, "two");
    pipeFiller(p, "three");
    pipeFiller(p, "four");
    pipeFiller(p, "five");
    List<ApplyType> to = p.getTo();
    assertTrue(to.get(0).getApply().equalsIgnoreCase("one"));
    assertTrue(to.get(1).getApply().equalsIgnoreCase("two"));
    assertTrue(to.get(2).getApply().equalsIgnoreCase("three"));
    assertTrue(to.get(3).getApply().equalsIgnoreCase("four"));
    assertTrue(to.get(4).getApply().equalsIgnoreCase("five"));
  }

  private void pipeFiller(PipelineType p, String type) {
    ApplyType applyType = new ApplyType();
    applyType.setApply(type);
    p.getTo().add(applyType);
  }
}
