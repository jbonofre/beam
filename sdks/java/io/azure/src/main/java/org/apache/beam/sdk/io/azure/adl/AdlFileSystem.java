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
package org.apache.beam.sdk.io.azure.adl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapts Azure filesystem connectors to be used as Apache Beam {@link FileSystem FileSystems}.
 */
public class AdlFileSystem extends FileSystem<AdlResourceId> {

  private static final Logger LOG = LoggerFactory.getLogger(AdlFileSystem.class);

  //Azure Data Lake minimum Buffer size: 4MB
  // private static final int MINIMUM_UPLOAD_BUFFER_SIZE_BYTES = 4 * 1024 * 1024;

  private ADLStoreClient adlStoreClient;

  AdlFileSystem(AdlFileSystemOptions options) throws Exception {
    checkNotNull(options, "options");

    if (Strings.isNullOrEmpty(options.getAadAuthEndpoint())) {
      LOG.info(
          "The Azure Data Lake Beam extension was included in this build,"
                 + "but the AAD Auth Endpoint was not specified. If you don't"
                 + " plan to use Azure Data Lake, then ignore this message.");
    }

    AccessTokenProvider provider =
            new ClientCredsTokenProvider(options.getAadAuthEndpoint(),
                                    options.getAadClientId(),
                                    options.getAadClientSecret());
    System.out.println("created access token provider");

    adlStoreClient = ADLStoreClient
            .createClient(new URI(options.getAdlInputURI()).getHost(), provider);
    System.out.println("created client");
  }

  @Override
  protected ReadableByteChannel open(AdlResourceId resourceId) throws IOException {
    return new AdlReadableSeekableByteChannel(adlStoreClient, resourceId);
  }


  @Override
  protected List<MatchResult> match(List<String> specs) {
    //TODO - match for ADL glob's
    ImmutableList.Builder<MatchResult> matchResults = ImmutableList.builder();
    return matchResults.build();
  }

  @AutoValue
  abstract static class ExpandedGlob {
    // TODO Confirm for ADLS
    abstract AdlResourceId getGlobPath();

    @Nullable
    abstract List<AdlResourceId> getExpandedPaths();

    @Nullable
    abstract IOException getException();

    static ExpandedGlob create(AdlResourceId globPath, List<AdlResourceId> expandedPaths) {
      checkNotNull(globPath, "globPath");
      checkNotNull(expandedPaths, "expandedPaths");
      return new AutoValue_AdlFileSystem_ExpandedGlob(globPath, expandedPaths, null);
    }

    static ExpandedGlob create(AdlResourceId globPath, IOException exception) {
      checkNotNull(globPath, "globPath");
      checkNotNull(exception, "exception");
      return new AutoValue_AdlFileSystem_ExpandedGlob(globPath, null, exception);
    }

  }

  @AutoValue
  abstract static class PathWithEncoding {
//TODO Confirm for ADLS
    abstract AdlResourceId getPath();

    @Nullable
    abstract String getContentEncoding();

    @Nullable
    abstract IOException getException();

    static PathWithEncoding create(AdlResourceId path, String contentEncoding) {
      checkNotNull(path, "path");
      checkNotNull(contentEncoding, "contentEncoding");
      return new AutoValue_AdlFileSystem_PathWithEncoding(path, contentEncoding, null);
    }

    static PathWithEncoding create(AdlResourceId path, IOException exception) {
      checkNotNull(path, "path");
      checkNotNull(exception, "exception");
      return new AutoValue_AdlFileSystem_PathWithEncoding(path, null, exception);
    }
  }

  /*
  private ExpandedGlob expandGlob(AdlResourceId glob) {
    //TODO Build for ADLS
    return ExpandedGlob.create(glob, expandedPaths.build());
  }

  private PathWithEncoding getPathContentEncoding(S3ResourceId path) {
    //TODO Build for ADLS
    return PathWithEncoding.create(path, Strings.nullToEmpty('NOT CREATED for ADLS YET');
  }

  private List<MatchResult> matchNonGlobPaths(Collection<AdlResourceId> paths) throws IOException {
        //TODO Build for ADLS
    List<Callable<MatchResult>> tasks = new ArrayList<>(paths.size());
    return callTasks(tasks);
  }
*/

  @Override
  protected WritableByteChannel create(AdlResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    //TODO Build for ADLS
    LOG.debug("creating file {}", resourceId);
    String filePath = resourceId.getPath().toString();
    /*
    ADLStoreClient client = getClient();
    OutputStream stream = client.createFile(filePath, IfExists.OVERWRITE);
    return Channels.newChannel(
        new BufferedOutputStream(client.createFile(filePath, IfExists.OVERWRITE)));
    */
    // TODO return a concrete channel
    return null;
  }


  @Override protected void copy(List srcResourceIds, List destResourceIds) throws IOException {
    //TODO Build for ADLS
  }

  @Override protected void rename(List srcResourceIds, List destResourceIds) throws IOException {
    //TODO Build for ADLS
  }

  @Override protected void delete(Collection<AdlResourceId> resourceIds) throws IOException {
        //TODO Build for ADLS
    for (AdlResourceId resourceId : resourceIds) {
      try {
        Files.delete(resourceId.getPath());
      } catch (NoSuchFileException e) {
        LOG.info("Ignoring failed deletion of file {} which already does not exist: {}", resourceId,
            e);
      }
    }
  }

  @Override protected AdlResourceId matchNewResource(String singleResourceSpec, boolean isDir) {
    Path path = Paths.get(singleResourceSpec);
    return AdlResourceId.fromPath(path, isDir);
  }

  @Override protected String getScheme() {
    return AdlResourceId.SCHEME;
  }

  /*
  @VisibleForTesting
  void setAdlFileSystem(ADLStoreClient adlStoreClientnAdl) {
    this.adlStoreClientnAdl = adlStoreClientnAdl;
  }
  */
}
