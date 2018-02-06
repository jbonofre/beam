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

import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities.getFileResource;

import java.io.File;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.beam.sdk.extensions.dsls.xml.flow.loader.exception.FlowDescriptorValidationException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.FlowDefinitionType;
import org.xml.sax.SAXException;

/**
 * This class responsible for unmarshalling the user defined input data flow definition file and
 * convert to into internal POJO. The user defined flow XML file contains the pipeline structure and
 * its participating entities definition. To see the structure of the flow definition please see the
 * beamflow.xsd.
 */
public class FlowUnmashaller {

  // Schema for the flow DSL
  private final Schema flowDSLSchema;

  /** Constructor this class locates the schema and load it. */
  public FlowUnmashaller() {
    String schemaTypeSchemaFile = getFileResource("/beamflow.xsd");
    flowDSLSchema = getSchema(schemaTypeSchemaFile);
  }

  /*
   * This method loads the beam flow DSL XSD file and returns Schema object to the caller
   */
  private Schema getSchema(String schemaTypeSchemaFile) {
    try {
      return SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
          .newSchema(getClass().getClassLoader().getResource("beamflow.xsd"));
    } catch (SAXException e) {
      throw new IllegalStateException("Could not initilize the flow DSL schema.", e);
    }
  }

  /**
   * A method that returns the instance of {@link FlowDefinitionType} contains the mapped values of
   * flow XML file defined by the user. The user defined flow XML file contains the pipeline
   * structure and its participating entities definition. To see the structure of the flow
   * definition please see the beamflow.xsd.
   *
   * @param flowDefinition is a {@link File} object of user defined input flow xml file.
   * @return a instance of {@link FlowDefinitionType} object to the caller.
   * @throws FlowDescriptorValidationException thors on
   */
  public FlowDefinitionType getFlowDefinition(File flowDefinition)
      throws FlowDescriptorValidationException {
    return unmarshelFlowXml(flowDefinition);
  }

  /**
   * This method unmarshall the user defined flow XML into an equivalent POJO representation using
   * JAXB.
   *
   * @param flowXml is the user defined flow definition file.
   * @return an instance of {@link FlowDefinitionType} object on successful completion of this
   *     method.
   * @throws FlowDescriptorValidationException if system failed to unmarshall the input flow XML
   *     file.
   */
  private FlowDefinitionType unmarshelFlowXml(final File flowXml)
      throws FlowDescriptorValidationException {
    try {
      final Unmarshaller unmarshaller = configure();
      final JAXBElement<FlowDefinitionType> unmarshal =
          unmarshaller.unmarshal(new StreamSource(flowXml), FlowDefinitionType.class);
      return unmarshal.getValue();

    } catch (JAXBException e) {
      throw new FlowDescriptorValidationException(
          "Failed process the input flow xml file " + flowXml, e);
    }
  }

  /**
   * A method to setup the basic JAXB marshaler for flow xml to POJO mappings.
   *
   * @return a fully configured JAXB {@link Unmarshaller} instance to the caller.
   * @throws JAXBException if there is validation or initialization.
   */
  private Unmarshaller configure() throws JAXBException {
    checkState(flowDSLSchema != null, "Flow Unmarsheller is not initialized.");
    // create a JAXBContext capable of handling classes generated into
    // the org.apache.beam.sdk.extensions.dsls.xml.flow.metadata package
    JAXBContext context = JAXBContext.newInstance(FlowDefinitionType.class);
    // create an Unmarshaller
    Unmarshaller unmarshaller = context.createUnmarshaller();
    unmarshaller.setEventHandler(new javax.xml.bind.helpers.DefaultValidationEventHandler());
    unmarshaller.setSchema(flowDSLSchema);
    return unmarshaller;
  }
}
