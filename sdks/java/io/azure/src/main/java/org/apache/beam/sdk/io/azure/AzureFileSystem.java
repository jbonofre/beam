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

//ADLS DEPENDENCIES
import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
//NATIVE APP
//import com.microsoft.azure.datalake.store.oauth2.DeviceCodeTokenProvider;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
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
//import java.nio.file.PathMatcher;
import java.nio.file.Paths;
//import java.nio.file.StandardCopyOption;
//import java.util.Arrays;
import java.util.Collection;
//import java.util.Collections;
import java.util.List;
//import java.util.regex.Matcher;


import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
//import org.apache.beam.sdk.io.fs.ResourceId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Adapts Azure filesystem connectors to be used as Apache Beam {@link FileSystem FileSystems}.
 */
public class AzureFileSystem extends FileSystem<AzureResourceId> {
  private static final Logger LOG = LoggerFactory.getLogger(AzureFileSystem.class);

  // This needs to be filled in for the app to work
  // full account FQDN, not just the account name
  private static String accountFQDN = "YOURLAKE.azuredatalake.net";
  private static String cId = "Data lake APP ID"; //Line limit of 100
  //line limit 100
  private static String authTokenRoot = "https://login.microsoftonline.com/";
  private static String authTokenChild = "TENANT ID/oauth2/token";
  private static String authTokenUrl = authTokenRoot + authTokenChild;
  private static String cKey = "KEY FROM WEB APP AZURE AD SET UP";

  //using NATIVE azure app authentication
  private static String nativeAppId = "IF USING A NATIVE APP IN AZURE AD";

  AzureFileSystem() {
  }

  private static void printExceptionDetails(ADLException ex) {
    System.out.println("ADLException:");
    System.out.format("  Message: %s%n", ex.getMessage());
    System.out.format("  HTTP Response code: %s%n", ex.httpResponseCode);
    System.out.format("  Remote Exception Name: %s%n", ex.remoteExceptionName);
    System.out.format("  Remote Exception Message: %s%n", ex.remoteExceptionMessage);
    System.out.format("  Server Request ID: %s%n", ex.requestId);
    System.out.println();
  }

  private static void printDirectoryInfo(DirectoryEntry ent) {
    System.out.format("Name: %s%n", ent.name);
    System.out.format("  FullName: %s%n", ent.fullName);
    System.out.format("  Length: %d%n", ent.length);
    System.out.format("  Type: %s%n", ent.type.toString());
    System.out.format("  Group: %s%n", ent.group);
    System.out.format("  User: %s%n", ent.user);
    System.out.format("  Permission: %s%n", ent.permission);
    System.out.format("  mtime: %s%n", ent.lastModifiedTime.toString());
    System.out.format("  atime: %s%n", ent.lastAccessTime.toString());
    System.out.println();
  }

  private static byte[] getSampleContent() {
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(s);
    out.println("This is a line");
    out.println("This is another line");
    out.println("This is yet another line");
    out.println("This is yet yet another line");
    out.println("This is yet yet yet another line");
    out.println("... and so on, ad infinitum");
    out.println();
    out.close();
    return s.toByteArray();
  }

  private ADLStoreClient getClient(){
    // Create client object using client creds
    AccessTokenProvider provider = new ClientCredsTokenProvider(authTokenUrl, cId, cKey);

    System.out.println("created provider");
    ADLStoreClient client = ADLStoreClient.createClient(accountFQDN, provider);
    System.out.println("created client");
    return client;
  }

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
    /*File absoluteFile = resourceId.getPath().toFile().getAbsoluteFile();
    if (absoluteFile.getParentFile() != null
        && !absoluteFile.getParentFile().exists()
        && !absoluteFile.getParentFile().mkdirs()
        && !absoluteFile.getParentFile().exists()) {
      throw new IOException("Unable to create parent directories for '" + resourceId + "'");
    }
    return Channels.newChannel(
        new BufferedOutputStream(new FileOutputStream(absoluteFile)));*/
  }

  @Override protected ReadableByteChannel open(AzureResourceId resourceId) throws IOException {
    LOG.debug("opening file {}", resourceId);
    @SuppressWarnings("resource") // The caller is responsible for closing the channel.
    // Create client object using client creds
    ADLStoreClient client = getClient();

    // create directory
    //client.createDirectory("/a/b/w");
    //System.out.println("created directory");

    // create file and write some content
    String filename = resourceId.getPath().toString();

    // Read File
    InputStream in = client.getReadStream(filename);
    return Channels.newChannel(in);
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

  @Override protected String getScheme() {
    return "adl";
  }
}
