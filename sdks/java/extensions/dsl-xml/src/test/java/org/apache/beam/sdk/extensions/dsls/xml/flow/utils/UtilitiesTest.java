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
package org.apache.beam.sdk.extensions.dsls.xml.flow.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.exception.PipelineDefinitionException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.AttributeType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputsType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.ObjectFactory;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputsType;
import org.junit.Before;
import org.junit.Test;



/** A test cases for the {@link Utilities} class. */
public class UtilitiesTest {

  private ObjectFactory factory;

  private InputType createInputs(InputType createInputType, String name, String uri) {
    createInputType.setName(name);
    createInputType.setUri(uri);
    return createInputType;
  }

  private OutputType createOutputs(OutputType createInputType, String name, String uri) {
    createInputType.setName(name);
    createInputType.setUri(uri);
    return createInputType;
  }

  @Before
  public void setup() {
    factory = new ObjectFactory();
  }

  @Test
  public void testGetBooleanProperty() {

    List<AttributeType> attribute = new ArrayList<>();
    attribute.add(setProps("compression.type", "gz"));
    attribute.add(setProps("other", "2"));
    attribute.add(setProps("test", "true"));
    Map<String, String> properties = Utilities.getProperties(attribute);
    assertEquals(true, Utilities.getBooleanProperty(properties, "test"));
  }

  private AttributeType setProps(String name, String value) {
    AttributeType attributeType = new AttributeType();
    attributeType.setName(name);
    attributeType.setValue(value);
    return attributeType;
  }

  @Test
  public void testGetClassStepType() {}

  @Test
  public void testGetDecodedURI() {
    BeamUri decodedURI = Utilities.getDecodedURI("kafka://topic2?format=avro");
    assertNotNull(decodedURI);
    assertEquals("kafka", decodedURI.getScheme());
    assertEquals("topic2", decodedURI.getPath());
    assertEquals("format=avro", decodedURI.getFormat());
  }

  @Test
  public void testGetIntProperty() {
    List<AttributeType> attribute = new ArrayList<>();
    attribute.add(setProps("compression.type", "gz"));
    attribute.add(setProps("other", "2"));
    attribute.add(setProps("test", "true"));
    Map<String, String> properties = Utilities.getProperties(attribute);
    int intProperty = Utilities.getIntProperty(properties, "other");
    assertEquals(2, intProperty);
  }

  @Test(expected = NumberFormatException.class)
  public void testGetIntPropertyException() {
    List<AttributeType> attribute = new ArrayList<>();
    attribute.add(setProps("compression.type", "gz"));
    attribute.add(setProps("other", "wrong"));
    attribute.add(setProps("test", "true"));
    Utilities.getIntProperty(Utilities.getProperties(attribute), "other");
  }

  @Test(expected = PipelineDefinitionException.class)
  public void testlookupFlowEntities() throws PipelineDefinitionException {
    InputsType createInputsType = factory.createInputsType();
    InputType test1 = createInputs(factory.createInputType(), "Test1", "kafka://topic?format=avro");
    createInputsType.getInput().add(test1);
    createInputsType
        .getInput()
        .add(createInputs(factory.createInputType(), "Test2", "kafka://topic2?format=avro"));
    createInputsType
        .getInput()
        .add(createInputs(factory.createInputType(), "Test3", "kafka://topic2?format=avro"));
    Map<String, InputType> mapInputsByName = Utilities.mapInputsByName(createInputsType.getInput());
    assertEquals(3, mapInputsByName.keySet().size());
    Utilities.lookupFlowEntities(mapInputsByName, "test4", InputType.class.getName());
  }

  @Test
  public void testMapInputsByName() {
    InputsType createInputsType = factory.createInputsType();
    InputType test1 = createInputs(factory.createInputType(), "Test1", "kafka://topic?format=avro");
    createInputsType.getInput().add(test1);
    InputType test2 =
        createInputs(factory.createInputType(), "Test2", "kafka://topic2?format=avro");
    createInputsType.getInput().add(test2);
    InputType test3 =
        createInputs(factory.createInputType(), "Test3", "kafka://topic2?format=avro");
    createInputsType.getInput().add(test3);
    Map<String, InputType> mapInputsByName = Utilities.mapInputsByName(createInputsType.getInput());
    assertEquals(3, mapInputsByName.keySet().size());
    assertEquals(test1, mapInputsByName.get("Test1"));
    assertEquals(test2, mapInputsByName.get("Test2"));
    assertEquals(test3, mapInputsByName.get("Test3"));
  }

  @Test
  public void testMapOutputByName() {
    OutputsType createOutputsType = factory.createOutputsType();
    OutputType test1 =
        createOutputs(factory.createOutputType(), "Test1", "kafka://topic?format=avro");
    createOutputsType.getOutput().add(test1);
    OutputType test2 = createOutputs(factory.createOutputType(), "Test2", "gs://data/location/*");
    createOutputsType.getOutput().add(test2);
    OutputType test3 =
        createOutputs(factory.createOutputType(), "Test3", "file://tmp/dir1/dir2/myfile.gzip");
    createOutputsType.getOutput().add(test3);
    Map<String, OutputType> mapOutputsByName =
        Utilities.mapOutputByName(createOutputsType.getOutput());
    assertEquals(3, mapOutputsByName.keySet().size());
    assertEquals(test1, mapOutputsByName.get("Test1"));
    assertEquals(test2, mapOutputsByName.get("Test2"));
    assertEquals(test3, mapOutputsByName.get("Test3"));
  }

  @Test
  public void testMapOutputsByUriScheme() {
    OutputsType createOutputsType = factory.createOutputsType();
    OutputType test1 =
        createOutputs(factory.createOutputType(), "Test1", "kafka://topic?format=avro");
    createOutputsType.getOutput().add(test1);
    OutputType test2 =
        createOutputs(factory.createOutputType(), "Test2", "file://root/location/sample.txt");
    createOutputsType.getOutput().add(test2);
    OutputType test3 =
        createOutputs(factory.createOutputType(), "Test3", "gs://diretory/localtion/*");
    createOutputsType.getOutput().add(test3);
    Map<String, OutputType> mapOutputsByName =
        Utilities.mapOutputsByUriScheme(createOutputsType.getOutput());
    assertEquals(3, mapOutputsByName.keySet().size());
    assertEquals(test1, mapOutputsByName.get("kafka"));
    assertEquals(test2, mapOutputsByName.get("file"));
    assertEquals(test3, mapOutputsByName.get("gs"));
  }

  @Test
  public void testMapStepsByName() {}
}
