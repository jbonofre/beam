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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.beam.sdk.extensions.dsls.xml.flow.loader.FlowUnmashaller;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.exception.PipelineDefinitionException;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.AttributeType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.InputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.OutputType;
import org.apache.beam.sdk.extensions.dsls.xml.flow.metadata.model.StepType;
import org.apache.beam.sdk.io.TextIO.CompressionType;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/** This class contains the utility methods. */
public class Utilities {
  private static final Logger LOG = LoggerFactory.getLogger(Utilities.class);
  /**
   * Method to get a file resource from the class path.
   *
   * @param resourceName The resource name
   * @return The file name
   */
  public static String getFileResource(String resourceName) {
    checkArgument(resourceName != null, " Resource name cannot be null.");
    try {
      URLClassLoader classLoader = (URLClassLoader) FlowUnmashaller.class.getClassLoader();

      String[] fileParts = resourceName.split("/");
      // Read the resource
      URL fileURL = classLoader.getResource(resourceName);

      // Check if the resource is defined
      if (fileURL != null) {
        // Return the resource as a file name
        LOG.debug("Found the resource file {}.", resourceName);
        return fileURL.getFile();
      } else {
        fileURL = classLoader.getResource(fileParts[fileParts.length - 1]);
        if (fileURL == null) {
          return null;
        }
        LOG.debug("Found the resource file {}.", resourceName);
        return fileURL.getFile();
      }
    } catch (Exception e) {
      LOG.error("Could not get file resource: " + resourceName + ':' + e.getMessage(), e);
      return null;
    }
  }

  /**
   * A utility method to parse URI string.
   *
   * @param uri The string to be parsed into a {@link URI}
   * @throws URISyntaxException If the given string violates standards.
   */
  public static BeamUri getDecodedURI(String uri) {
    return URIUtil.translatUri(uri);
  }

  /**
   * This method maps steps by name and return {@link Map} to the caller.
   *
   * @param values a collection of StepTpe object that represents each steps defined in the flow
   *     dsl.
   */
  public static Map<String, StepType> mapStepsByName(List<StepType> values) {
    checkArgument(values != null, "User defined data steps are not configured correctly.");
    checkArgument(values.size() != 0, "User defined data steps are not configured correctly.");
    return Maps.uniqueIndex(
        values,
        new Function<StepType, String>() {

          public String apply(StepType type) {
            return type.getName().trim().toLowerCase();
          }
        });
  }

  /**
   * This method maps InputType by URI scheme and return {@link Map} to the caller.
   *
   * @param values a collection of InputType object that represents each steps defined in the flow
   *     dsl.
   */
  public static Map<String, InputType> mapInputsByUriScheme(@Nonnull List<InputType> values) {
    return Maps.uniqueIndex(
        values,
        new Function<InputType, String>() {

          public String apply(InputType type) {
            return Utilities.getDecodedURI(type.getUri()).getScheme();
          }
        });
  }
  /**
   * This method maps OutputType by URI scheme and return {@link Map} to the caller.
   *
   * @param values a collection of OutputType object that represents each steps defined in the flow
   *     dsl.
   */
  public static Map<String, OutputType> mapOutputsByUriScheme(@Nonnull List<OutputType> values) {
    return Maps.uniqueIndex(
        values,
        new Function<OutputType, String>() {

          public String apply(OutputType type) {
            return Utilities.getDecodedURI(type.getUri()).getScheme();
          }
        });
  }

  /**
   * This method maps InputType by name and return {@link Map} to the caller.
   *
   * @param values a collection of Step object that represents each steps defined in the flow dsl.
   */
  public static Map<String, InputType> mapInputsByName(@Nonnull List<InputType> values) {
    return Maps.uniqueIndex(
        values,
        new Function<InputType, String>() {

          public String apply(InputType type) {
            return type.getName().trim();
          }
        });
  }

  /**
   * This method maps OutputType by name and return {@link Map} to the caller.
   *
   * @param values a collection of StepTpe object that represents each steps defined in the flow
   *     dsl.
   */
  public static Map<String, OutputType> mapOutputByName(List<OutputType> values) {
    return Maps.uniqueIndex(
        values,
        new Function<OutputType, String>() {

          public String apply(OutputType type) {
            return type.getName().trim();
          }
        });
  }

  /**
   * This is utility method defined to enable flow entity validation only.
   *
   * @throws PipelineDefinitionException if the return object does not exist in the supplied map
   *     object.
   */
  public static <V> V lookupFlowEntities(
      Map<String, V> mappedValues, String lookup, String itemType)
      throws PipelineDefinitionException {
    V v = mappedValues.get(lookup);
    if (v == null) {
      throw new PipelineDefinitionException(
          "The " + itemType + " " + lookup + " is not defined in the flow DSL.");
    }
    return v;
  }

  public static Class<?> getClass(StepType metadata) {

    try {
      return ReflectHelpers.findClassLoader().loadClass(metadata.getHandlerClass());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to load   "
              + metadata.getName()
              + " class "
              + metadata.getHandlerClass()
              + "defined in the flow xml file.");
    }
  }

  public static Map<String, String> getProperties(@Nonnull List<AttributeType> attribute) {
    if (attribute == null) {
      return new HashMap<>();
    }
    final Map<String, String> props = new HashMap<>();
    for (AttributeType attributeType : attribute) {
      props.put(attributeType.getName().trim(), attributeType.getValue().trim());
    }
    return props;
  }

  public static Boolean getBooleanProperty(Map<String, String> properties, String property) {
    return new Boolean(properties.get(property));
  }

  public static Integer getIntProperty(Map<String, String> properties, String property) {
    String value = properties.get(property);
    if (value == null) {
      return null;
    }
    return new Integer(value);
  }

  public static Long getLongProperty(Map<String, String> properties, String property) {
    return new Long(properties.get(property));
  }

  /**
   * Possible text file compression types.
   *
   * @param uri
   * @param compressionTypeExplicitlyDefined
   * @return
   */
  public static CompressionType getCompressionType(
      BeamUri uri, String compressionTypeExplicitlyDefined) {
    String compressionTypeFromFile = URIUtil.getFileType(uri.getFormat());
    if (compressionTypeFromFile == null) {
      if (compressionTypeExplicitlyDefined != null) {
        compressionTypeFromFile = compressionTypeExplicitlyDefined;
      } else {
        return CompressionType.AUTO;
      }
    }
    switch (compressionTypeFromFile) {
      case "uncompressed":
        return CompressionType.UNCOMPRESSED;
      case "none":
        return CompressionType.AUTO;
      case "gzip":
        return CompressionType.GZIP;
      case "bzip2":
        return CompressionType.BZIP2;
      case "zip":
        return CompressionType.ZIP;
      case "deflate":
        return CompressionType.DEFLATE;

      default:
        return CompressionType.AUTO;
    }
  }

  /** A method to create the instance of class supplied. {@link PipelineDefinitionException} */
  public static Object createInstance(Class<?> funClass) throws PipelineDefinitionException {
    checkArgument(funClass != null, "Step handler class cannot be null.");
    try {
      return funClass.newInstance();
    } catch (Exception e) {
      throw new PipelineDefinitionException(
          "Failed to instantiate handler class "
              + funClass.getName()
              + "defined in the flow xml file, reason.",
          e);
    }
  }

  /**
   * A method that loads the Apache Kafka {@link Deserializer} classes for the Beam {@link KafkaIO}
   * adapter.
   *
   * @param className is the absolute class name of the Apache Kafka {@link Deserializer} class.
   * @return an {@link ImmutablePair} of object contains a mapped value of the {@link Deserializer}
   *     type and actual class.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static ImmutablePair<String, Class<? extends Deserializer<?>>> getDeserializer(
      String className) {
    String classNaeWithoutExtension =
        className.endsWith(".class")
            ? className.substring(0, className.indexOf(".class"))
            : className;
    String packegeName =
        classNaeWithoutExtension.substring(0, classNaeWithoutExtension.lastIndexOf("."));
    Reflections reflections = new Reflections(packegeName);
    Set<Class<? extends Deserializer>> classes = reflections.getSubTypesOf(Deserializer.class);
    for (Class<? extends Deserializer> eachClass : classes) {
      if (!className.equals(eachClass.getName())) {
        continue;
      }
      Type[] genericInterfaces = eachClass.getGenericInterfaces();
      String typeName = genericInterfaces[0].getTypeName();
      String type = typeName.substring(typeName.indexOf('<') + 1, typeName.indexOf(">"));
      return new ImmutablePair<String, Class<? extends Deserializer<?>>>(
          type, (Class<? extends Deserializer<?>>) eachClass);
    }
    throw new RuntimeException("Deserializer class " + className + " could not found.");
  }

  /**
   * A method that converts a string or sequence of comma separated string names into a {@link List}
   * .
   */
  public static List<String> getTopics(String kafkaTopicsRequested) {
    checkArgument(kafkaTopicsRequested != null && !kafkaTopicsRequested.isEmpty());
    return kafkaTopicsRequested.contains(",")
        ? toList(kafkaTopicsRequested.split(","))
        : toList(new String[] {kafkaTopicsRequested.trim()});
  }

  private static List<String> toList(String[] split) {
    checkArgument(split != null && split.length > 0);
    return Arrays.asList(split);
  }

  public static Class<? extends Deserializer<?>> getDeserializerClass(String keyDeserializer) {

    Class<? extends Deserializer<?>> classLoaded = StringDeserializer.class;
    return classLoaded;
  }
}
