<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Example Pipelines- DSL Declarative API

The examples included in this module serve to demonstrate the basic
functionality of Apache Beam, and act as starting points for
the development of more complex pipelines.

## Word Count

Few examples using Apache Beam Declarative API.

1. [`WordCount`]is the simplest word count pipeline and introduces basic beam DSL https://github.com/jbonofre/beam/blob/DSL_XML/dsls/xml/src/resources/BeamFlow-File-in-File-out.xml

## Running Examples

#### How to run the example with DSL
##### Run with direct runner
 mvn compile exec:java -Dexec.mainClass=org.apache.beam.sdk.extensions.dsls.xml.flow.loader.BeamFlowDSLRunner -Dexec.args="--dslXml=**src/main/resources/BeamFlow-File-in-File-out.xml**" -Pdirect-runner
##### Run with Spark runner
 mvn compile exec:java -Dexec.mainClass=org.apache.beam.sdk.extensions.dsls.xml.flow.loader.BeamFlowDSLRunner -Dexec.args="--dslXml=**src/main/resources/BeamFlow-File-in-File-out.xml** " -Pspark-runner
##### Run with Flink runner 
 mvn compile exec:java -Dexec.mainClass=org.apache.beam.sdk.extensions.dsls.xml.flow.loader.BeamFlowDSLRunner -Dexec.args="--dslXml=**src/main/resources/BeamFlow-File-in-File-out.xml** " -Pflink-runner
##### Run with Apex runner 
 mvn compile exec:java -Dexec.mainClass=org.apache.beam.sdk.extensions.dsls.xml.flow.loader.BeamFlowDSLRunner -Dexec.args="--dslXml=**src/main/resources/BeamFlow-File-in-File-out.xml** " -Papex-runner
