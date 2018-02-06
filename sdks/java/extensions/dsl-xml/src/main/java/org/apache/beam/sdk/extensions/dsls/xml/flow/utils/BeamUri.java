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

import java.io.Serializable;

import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.IOConfiguration;

/** A Beam bean class the represents URI entities that mapped to meet the DSL requirements. */
public final class BeamUri implements Serializable {

  /** Generated Serial version id. */
  private static final long serialVersionUID = -6276256886103184364L;

  /** The protocol part of the URI. */
  private final String scheme;
  /**
   * the location of data - it could be a directory or kafka topic or hive table or absolute path of
   * a file.
   */
  private String path;
  /** This is option element of the URI represents the format of the data. */
  private final String format;

  /** A constructor of this class. */
  public BeamUri(String scheme, String path, String format) {
    super();
    this.scheme = scheme;
    this.path = path;
    this.format = format;
  }

  public String getScheme() {
    return scheme;
  }

  /**
   * @see https://tools.ietf.org/html/rfc3986#section-3.3 A path consists of a sequence of path
   *     segments separated by a slash ("/") character. A path is always defined for a URI, though
   *     the defined path may be empty (zero length). Use of the slash character to indicate
   *     hierarchy is only required when a URI will be used as the context for relative references.
   *     For example, the URI "mailto:fred@example.com" has a path of "fred@example.com", whereas
   *     the URI "foo://info.example.com?fred" has an empty path.

   *     <p>The path segments "." and "..", also known as dot-segments, are defined for relative
   *     reference within the path name hierarchy. They are intended for use at the beginning of a
   *     relative-path reference (Section 4.2) to indicate relative position within the hierarchical
   *     tree of names. This is similar to their role within some operating systems' file directory
   *     structures to indicate the current directory and parent directory, respectively. However,
   *     unlike in a file system, these dot-segments are only interpreted within the URI path
   *     hierarchy and are removed as part of the resolution process (Section 5.2).</p>

   *     <p>Aside from dot-segments in hierarchical paths, a path segment is considered opaque by
   *     the generic syntax. URI producing applications often use the reserved characters allowed in
   *     a segment to delimit scheme-specific or dereference-handler-specific subcomponents. For
   *     example, the semicolon (";") and equals ("=") reserved characters are often used to delimit
   *     parameters and parameter values applicable to that segment. The comma (",") reserved
   *     character is often used for similar purposes. For example, one URI producer might use a
   *     segment such as "name;v=1.1" to indicate a reference to version 1.1 of "name", whereas
   *     another might use a segment such as "name,1.1" to indicate the same. Parameter types may be
   *     defined by scheme-specific semantics, but in most cases the syntax of a parameter is
   *     specific to the implementation of the URI's dereferencing algorithm.</p>
   */
  public String getPath() {
    switch (scheme) {
      case IOConfiguration.FileIOConfiguration.SCHEME_TYPE:
        return path;
      case IOConfiguration.FileIOConfiguration.GS_SCHEME_TYPE:
        return path;
      case IOConfiguration.KafkaCommonConfiguaration.SCHEME_TYPE:
        return validateAndBuildKafkaPath(path);
      default:
        break;
    }
    return path;
  }

  private String validateAndBuildKafkaPath(String pathContext) {
    if (pathContext.startsWith("//")) {
      pathContext = pathContext.replace("//", "");
    }
    if (pathContext.indexOf("/") > 0) {
      if (pathContext.split("/").length > 1 || pathContext.endsWith("/")) {
        throw new InvalidBeamURIException(
            "Invalid kafka URI. Use the folowing syntax. \n kafka://"
                + "<topic-name>?format=\"<json/csv/avro/gbp>\"");
      }
    }
    if (pathContext.indexOf("/") > 0) {
      pathContext = pathContext.substring(0, pathContext.indexOf("/"));
    }
    return pathContext;
  }

  public String getFormat() {
    return format != null ? format.replace("?", "") : null;
  }

  public String getURI() {
    switch (scheme) {
      case "file":
        return path;

      default:
        return scheme + ":" + path + ((format == null || format.trim().isEmpty()) ? "" : format);
    }
  }

  @Override
  public String toString() {
    return "Beam Flow DSL URI [ " + scheme + "://" + path + format + " ]";
  }
}
