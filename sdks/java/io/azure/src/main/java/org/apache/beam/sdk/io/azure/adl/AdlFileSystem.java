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

//ADLS DEPENDENCIES
import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;


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

  private ADLStoreClient adlStoreClient;

  AdlFileSystem(AdlFileSystemOptions options) {
    checkNotNull(options, "options");

    if (Strings.isNullOrEmpty(options.getAadAuthEndpoint())) {
      LOG.info(
          "The Azure Data Lake Beam extension was included in this build, but the AAD Auth Endpoint "
              + "was not specified. If you don't plan to use Azure Data Lake, then ignore this message.");
    }

    AccessTokenProvider provider = 
       new ClientCredsTokenProvider(options.getAadAuthEndpoint(), 
                                    options.getAadClientId(),
                                    options.getAadClientSecret());
    System.out.println("created access token provider");

    adlStoreClient = ADLStoreClient.createClient(new URI(options.getAdlInputURI()).getHost(), provider);
    System.out.println("created client");
  }

  @Override
  protected ReadableByteChannel open(AdlResourceId resourceId) throws IOException {
    FileStatus fileStatus = fileSystem.getFileStatus(resourceId.toPath());
    return new AdlSeekableByteChannel(adlStoreClient, fileSystem.open(resourceId.toPath()));
  }

  /** An adapter around {@link FSDataInputStream} that implements {@link SeekableByteChannel}. */
  private static class AdlSeekableByteChannel implements SeekableByteChannel {
    private final FileStatus fileStatus;
    private final FSDataInputStream inputStream;
    private boolean closed;

    private AdlSeekableByteChannel(FileStatus fileStatus, FSDataInputStream inputStream) {
      this.fileStatus = fileStatus;
      this.inputStream = inputStream;
      this.closed = false;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (closed) {
        throw new IOException("Channel is closed");
      }
      // O length read must be supported
      int read = 0;
      // We avoid using the ByteBuffer based read for Hadoop because some FSDataInputStream
      // implementations are not ByteBufferReadable,
      // See https://issues.apache.org/jira/browse/HADOOP-14603
      if (dst.hasArray()) {
        // does the same as inputStream.read(dst):
        // stores up to dst.remaining() bytes into dst.array() starting at dst.position().
        // But dst can have an offset with its backing array hence the + dst.arrayOffset()
        read = inputStream.read(dst.array(), dst.position() + dst.arrayOffset(), dst.remaining());
      } else {
        // TODO: Add support for off heap ByteBuffers in case the underlying FSDataInputStream
        // does not support reading from a ByteBuffer.
        read = inputStream.read(dst);
      }
      if (read > 0) {
        dst.position(dst.position() + read);
      }
      return read;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException {
      if (closed) {
        throw new IOException("Channel is closed");
      }
      return inputStream.getPos();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
      if (closed) {
        throw new IOException("Channel is closed");
      }
      inputStream.seek(newPosition);
      return this;
    }

    @Override
    public long size() throws IOException {
      if (closed) {
        throw new IOException("Channel is closed");
      }
      return fileStatus.getLen();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
      return !closed;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      inputStream.close();
    }
  }

  /*
  @Override protected List<MatchResult> match(List specs) throws IOException {
    return null;
  }

  @Override
  protected WritableByteChannel create(AzureResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    LOG.debug("creating file {}", resourceId);
    String filePath = resourceId.getPath().toString();
    ADLStoreClient client = getClient();
    OutputStream stream = client.createFile(filePath, IfExists.OVERWRITE);
    return Channels.newChannel(
        new BufferedOutputStream(client.createFile(filePath, IfExists.OVERWRITE)));

  }


  @Override protected void copy(List srcResourceIds, List destResourceIds) throws IOException {

  }

  @Override protected void rename(List srcResourceIds, List destResourceIds) throws IOException {

  }

  @Override protected void delete(Collection<AzureResourceId> resourceIds) throws IOException {
    for (AzureResourceId resourceId : resourceIds) {
      try {
        Files.delete(resourceId.getPath());
      } catch (NoSuchFileException e) {
        LOG.info("Ignoring failed deletion of file {} which already does not exist: {}", resourceId,
            e);
      }
    }
  }

  @Override protected AzureResourceId matchNewResource(String singleResourceSpec, boolean isDir) {
    Path path = Paths.get(singleResourceSpec);
    return AzureResourceId.fromPath(path, isDir);
  }
*/
  @Override protected String getScheme() {
    return AdlResourceId.SCHEME;
  }

  @VisibleForTesting
  void setAdlFileSystem(ADLStoreClient adlStoreClientnS3) {
    this.adlStoreClientnS3 = adlStoreClientnS3;
  }
}
