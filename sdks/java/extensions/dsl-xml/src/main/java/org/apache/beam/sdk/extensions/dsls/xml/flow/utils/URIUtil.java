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

import javax.annotation.Nonnull;

/** A utility class for manipulating the URI information from the DSL. */
public class URIUtil {

  public static BeamUri translatUri(String uri) {
    return new BeamUri(getScheme(uri), getPath(uri), getQuery(uri));
  }

  /**
   * Get the basename of an URI. It's possibly an empty string.
   *
   * @param uri a string regarded an URI
   * @return the basename string; an empty string if the path ends with slash
   */
  public static String getScheme(@Nonnull String uri) {
    uriNotNull(uri);
    int indexOf = uri.indexOf("://");
    if (indexOf < 0) {
      error("URI cannot be null or empty.");
    }
    if (uri.length() <= (indexOf + 3)) {
      error("URI cannot be null or empty.");
    }
    String part = uri.substring(0, indexOf);
    return part;
  }

  private static void uriNotNull(String uri) {
    if (uri == null || uri.isEmpty()) {
      error("URI cannot be null or empty.");
    }
  }

  private static void error(String message) {
    throw new InvalidBeamURIException(message);
  }

  /**
   * Get the query of an URI.
   *
   * @param uri a string regarded an URI
   * @return the query string; null if empty or undefined
   */
  public static String getQuery(@Nonnull String uri) {
    uriNotNull(uri);
    if (uri.contains(".")) {
      if (uri.indexOf("?") > 0) {
        return uri.substring(uri.indexOf("?") + 1, uri.length());
      } else {
        return "format=" + uri.split("\\.")[1];
      }
    }
    // consider of net_path
    int at = uri.indexOf("//");
    int from = uri.indexOf("/", at >= 0 ? (uri.lastIndexOf("/", at - 1) >= 0 ? 0 : at + 2) : 0);
    // the authority part of URI ignored
    int to = uri.length();
    // reuse the at and from variables to consider the query
    at = uri.indexOf("?", from);
    if ((at + 1) == to) {
      error("Invalid URI, missing the data format element from URI");
    }
    if (at >= 0) {
      from = at + 1;
    } else {
      return null;
    }
    // check the fragment
    if (uri.lastIndexOf("#") > from) {
      to = uri.lastIndexOf("#");
    }
    // get the path and query.
    String ret = (from < 0 || from == to) ? null : uri.substring(from, to);
    if (!ret.matches("format=\\w.*")) {
      error("Invalid URI, missing the data format element from URI");
    }

    return ret;
  }

  /**
   * Get the path of an URI.
   *
   * @param uri a string regarded an URI
   * @return the path string
   */
  public static String getPath(@Nonnull String uri) {
    uriNotNull(uri);
    // consider of net_path
    int at = uri.indexOf("//");
    int from = uri.indexOf("/", at >= 0 ? (uri.lastIndexOf("/", at - 1) >= 0 ? 0 : at + 1) : 0);
    // the authority part of URI ignored
    int to = uri.length();
    // check the query
    if (uri.indexOf('?', from) != -1) {
      to = uri.indexOf('?', from);
    }
    // check the fragment
    if (uri.lastIndexOf("#") > from && uri.lastIndexOf("#") < to) {
      to = uri.lastIndexOf("#");
    }
    // get only the path.
    return (from < 0) ? (at >= 0 ? "/" : uri) : uri.substring(from - 1, to);
  }

  /**
   * Get the path and query of an URI.
   *
   * @param uri a string regarded an URI
   * @return the path and query string
   */
  public static String getPathQuery(String uri) {
    if (uri == null) {
      return null;
    }
    // consider of net_path
    int at = uri.indexOf("//");
    int from = uri.indexOf("/", at >= 0 ? (uri.lastIndexOf("/", at - 1) >= 0 ? 0 : at + 2) : 0);
    // the authority part of URI ignored
    int to = uri.length();
    // Ignore the '?' mark so to ignore the query.
    // check the fragment
    if (uri.lastIndexOf("#") > from) {
      to = uri.lastIndexOf("#");
    }
    // get the path and query.
    return (from < 0) ? (at >= 0 ? "/" : uri) : uri.substring(from, to);
  }

  public static String getFileType(@Nonnull String format) {
    if (format == null || format.trim().isEmpty()) {
      return null;
    }
    if (!format.contains("=")) {
      return null;
    }
    return format.trim().split("=")[1];
  }
}
