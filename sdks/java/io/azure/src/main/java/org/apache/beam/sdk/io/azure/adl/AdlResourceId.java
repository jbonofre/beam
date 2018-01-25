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

import com.google.common.base.Optional;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;

/**
 * {@link ResourceId} implementation for the {@link AdlFileSystem}.
 */
public class AdlResourceId implements ResourceId {

  static final String SCHEME = "adl";
  private final URI uri;

  AdlResourceId(URI uri) {
    this.uri = uri;
  }

  static AdlResourceId fromPath(Path path, boolean isDirectory) {
    checkNotNull(path, "path");
    return new AdlResourceId(new File(path.toString()).toURI());
  }

  @Override
  public ResourceId resolve(String other, ResolveOptions resolveOptions) {
    if (resolveOptions == ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY) {
      if (!other.endsWith("/")) {
        other += '/';
      }
      return new AdlResourceId(uri.resolve(other));
    } else if (resolveOptions == ResolveOptions.StandardResolveOptions.RESOLVE_FILE) {
      checkArgument(!other.endsWith("/"), "Resolving a file with a directory path: %s", other);
      return new AdlResourceId(uri.resolve(other));
    } else {
      throw new UnsupportedOperationException(
          String.format("Unexpected StandardResolveOptions %s", resolveOptions));
    }
  }

  @Override
  public ResourceId getCurrentDirectory() {
    return new AdlResourceId(uri.getPath().endsWith("/") ? uri : uri.resolve("."));
  }

  public boolean isDirectory() {
    return uri.getPath().endsWith("/");
  }
  Path getPath() {
    return Paths.get(new File(uri).getAbsolutePath());
  }

  @Override
  public String getFilename() {
    // TODO should be replace with Azure path
    return new File(uri).getName();
  }

  @Override
  public String getScheme() {
    return uri.getScheme();
  }

  @Override
  public String toString() {
    return uri.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AdlResourceId)) {
      return false;
    }
    return Objects.equals(uri, ((AdlResourceId) obj).uri);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(uri);
  }

  Optional<Long> getSize() {
    // TODO Extract size from ADLStoreClient.getContentSummary
    return Optional.fromNullable(0L);
  }
  /*
  Path toPath() {
    return new Path(uri);
  }
  */

}
