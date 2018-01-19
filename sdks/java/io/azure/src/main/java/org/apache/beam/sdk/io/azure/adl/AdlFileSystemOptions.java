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
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;
import com.microsoft.azure.datalake.store.DirectoryEntry;


import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * {@link PipelineOptions} which encapsulate Azure configuration for the file system.
 */
@Experimental(Experimental.Kind.FILESYSTEM)
public interface AdlFileSystemOptions extends PipelineOptions {

  @Description("Service Principal with access to Azure Data Lake Storage URL we are accessing - Azure Active Directory Authorizaion End Point")
  @Default.String("https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")
  String getAadAuthEndpoint();
  void setAadAuthEndpoint(String value);

  @Description("Service Principal with access to Azure Data Lake Storage URL we are accessing - Azure Active Directory Client ID")
  @Default.String("f336a26f-e14b-4f19-93ae-ff56b0aa9146")
  String getAadClientId();
  void setAadClientId(String value);

  @Description("Service Principal with access to Azure Data Lake Storage URL we are accessing - Azure Active Directory Client Secret")
  @Default.String("8bd03de9-dd3c-4211-aad6-e77ea3330257")
  String getAadClientSecret();
  void setAadClientSecret(String value);

  @Description("Azure Data Lake Input File URI - must start with 'adl://' ")
  @Default.String("adl://gidatalake1.azuredatalakestore.net/gisampledata/Drivers.txt")
  String getAdlInputURI();
  void setAdlInputURI(String value);
}
