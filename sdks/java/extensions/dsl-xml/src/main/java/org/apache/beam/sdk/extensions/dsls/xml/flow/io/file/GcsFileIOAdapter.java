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

package org.apache.beam.sdk.extensions.dsls.xml.flow.io.file;

import com.google.auto.service.AutoService;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.extensions.dsls.xml.flow.io.IOAdapter;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.file.config.GcsFileReaderConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.io.file.config.GcsFileWriterConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.util.gcsfs.GcsPath;


/**
 * Reads text files that reads file(s) with the given filename or filename pattern from google file
 * system.
 *
 * <p>Standard <a href="http://docs.oracle.com/javase/tutorial/essential/io/find.html" >Java
 * Filesystem glob patterns</a> ("*", "?", "[..]") are supported.
 */
@AutoService(IOAdapter.class)
public class GcsFileIOAdapter extends FileIOAdapter {

  /** Generated Serial version id. */
  private static final long serialVersionUID = -2575117153098957592L;

  /*
   * (non-Javadoc)
   *
   * @see org.apache.beam.sdk.extensions.dsls.xml.flow.io.IOAdapter#getHandlerSchema()
   */
  @Override
  public String getHandlerScheme() {
    return IOConfiguration.FileIOConfiguration.GS_SCHEME_TYPE;
  }

  public void init(@Nonnull InputType type) {
    super.init(type);
    fileReaderConiguration = GcsFileReaderConfiguration.build(getProperties(), getDecodedURI());
  }

  @Override
  public void init(@Nonnull OutputType type) {
    super.init(type);
    fileWriterConfiguration = GcsFileWriterConfiguration.build(getProperties(), getDecodedURI());
  }
  protected String getWritePath() {
    return GcsPath.fromUri(fileWriterConfiguration.getPath()).toString();
  }
}
