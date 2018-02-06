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

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Options that configure the {@link BeamFlowDSLRunner}. */
public interface FlowXmlOptions extends PipelineOptions {

  /**
   * By default, this example reads from a public data set containing the text of King Lear. Set
   * this option to choose a different input file or glob.
   */
  @Description("Path of the flow file to read from")
  //  @Default.String("src/main/resources/BeamFlowExample.xml")
  @Required
  String getDslXml();

  void setDslXml(String value);
}
