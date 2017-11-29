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

import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;

/**
 * Adapts Azure filesystem connectors to be used as Apache Beam {@link FileSystem FileSystems}.
 */
public class AzureFileSystem extends FileSystem {

  @Override protected List<MatchResult> match(List specs) throws IOException {
    return null;
  }

  @Override protected WritableByteChannel create(ResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    return null;
  }

  @Override protected ReadableByteChannel open(ResourceId resourceId) throws IOException {
    return null;
  }

  @Override protected void copy(List srcResourceIds, List destResourceIds) throws IOException {

  }

  @Override protected void rename(List srcResourceIds, List destResourceIds) throws IOException {

  }

  @Override protected void delete(Collection resourceIds) throws IOException {

  }

  @Override protected ResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    return null;
  }

  @Override protected String getScheme() {
    return null;
  }
}
