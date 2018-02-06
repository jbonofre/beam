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

package org.apache.beam.sdk.extensions.dsls.xml.flow.io;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ServiceLoader;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.BeamUri;
import org.apache.beam.sdk.extensions.dsls.xml.flow.utils.Utilities;


/**
 * This class is responsible loading the {@link IOAdapter} implementations that registered as a
 * service. Any new IOadapter provider must implement the {@link IOAdapter} and use the annotation
 * {@link AutoService} to mark the provide class as a service.
 */
final class IOServiceRegsitry implements Serializable {
  /** Generated Serial Version ID. */
  private static final long serialVersionUID = -7469950239861939614L;

  @SuppressWarnings("rawtypes")
  private static ServiceLoader<IOAdapter> ioAdapterServiceRegistry;

  static {
    ioAdapterServiceRegistry = ServiceLoader.load(IOAdapter.class);
  }

  @SuppressWarnings("rawtypes")
  static IOAdapter getIOAdapterService(@Nonnull final String uri) {
    final BeamUri decodedURI = Utilities.getDecodedURI(uri);

    for (IOAdapter ioAdapter : ioAdapterServiceRegistry) {
      if (ioAdapter.understand(decodedURI)) {
        return ioAdapter;
      }
    }
    throw new UnsupportedOperationException(
        "Requested Input type " + decodedURI.getScheme() + " is not supported.");
  }
}
