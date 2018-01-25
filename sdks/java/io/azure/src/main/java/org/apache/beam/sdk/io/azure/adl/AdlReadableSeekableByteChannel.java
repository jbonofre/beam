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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.microsoft.azure.datalake.store.ADLFileInputStream;
import com.microsoft.azure.datalake.store.ADLStoreClient;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

/**
 * A readable S3 object, as a {@link SeekableByteChannel}.
 */
public class AdlReadableSeekableByteChannel implements SeekableByteChannel {

  private ADLStoreClient adlStoreClient;
  private AdlResourceId path;
  private long contentLength = 0;
  private long position = 0;
  private boolean open = true;
  private ADLFileInputStream adlFileInputStream;
  private ReadableByteChannel adlContentChannel;

  AdlReadableSeekableByteChannel(ADLStoreClient adlStoreClient, AdlResourceId path) {
    this.adlStoreClient = checkNotNull(adlStoreClient, "adlStoreClient");
    this.path = checkNotNull(path, "path");

    /* TODO - do we need to validate / set size for ADLS?
    if (path.getSize().isPresent()) {
      contentLength = path.getSize().get();
      this.path = path;

    } else {
    this.path = path;
    }
     try {
        contentLength =
            amazonS3.getObjectMetadata(path.getBucket(), path.getKey()).getContentLength();

      } catch (AmazonClientException e) {
        throw new IOException(e);
      }
      this.path = path.withSize(contentLength);
    }
  }
  */
  }

  @Override
  public int read(ByteBuffer destinationBuffer) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    if (!destinationBuffer.hasRemaining()) {
      return 0;
    }
    if (this.position == contentLength) {
      return -1;
    }

    if (adlFileInputStream == null) {
      // ADLS Equivalent?
      // GetObjectRequest request = new GetObjectRequest(path.getBucket(), path.getKey());
      //TODO
      //- ADLS File read for position / length
      //- is desired position/length set via user file specificication?
      this.position = 0;
      try {
        adlFileInputStream = adlStoreClient.getReadStream(path.toString());
      } catch (IOException e) {
        throw new IOException(e);
      }
      adlContentChannel = Channels.newChannel(
          new BufferedInputStream(adlFileInputStream, 1024 * 1024));
    }

    int totalBytesRead = 0;
    int bytesRead = 0;

    do {
      totalBytesRead += bytesRead;
      try {
        bytesRead = adlContentChannel.read(destinationBuffer);
      } catch (IOException e) {
        // TODO what is the right exception here?
        throw new IOException(e);
      }
    } while (bytesRead > 0);

    position += totalBytesRead;
    return totalBytesRead;
  }

  @Override
  public long position() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    checkArgument(newPosition >= 0, "newPosition too low");
    checkArgument(newPosition < contentLength, "new position too high");

    if (newPosition == position) {
      return this;
    }

    position = newPosition;
    return this;
  }

  @Override
  public long size() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return contentLength;
  }

  @Override
  public void close() throws IOException {
    if (adlFileInputStream != null) {
      adlFileInputStream.close();
    }
    open = false;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public int write(ByteBuffer src) {
    throw new NonWritableChannelException();
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new NonWritableChannelException();
  }
}
