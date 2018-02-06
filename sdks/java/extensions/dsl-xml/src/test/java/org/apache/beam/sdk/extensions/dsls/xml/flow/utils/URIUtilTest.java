package org.apache.beam.sdk.extensions.dsls.xml.flow.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/** junit class class for the {@link URIUtil}. */
public class URIUtilTest {

  @Test
  public void testGetScheme() {
    assertEquals("kafka", URIUtil.getScheme("kafka://maypath/secondpath/thirdPath?format=avro"));
  }

  @Test(expected = InvalidBeamURIException.class)
  public void testGetSchemeIllegal() {
    URIUtil.getScheme("//maypath/secondpath/thirdPath?format=avro");
  }

  @Test(expected = InvalidBeamURIException.class)
  public void testGetSchemeIllegalPath() {
    URIUtil.getScheme("/maypath/secondpath/thirdPath?format=avro");
  }

  @Test(expected = InvalidBeamURIException.class)
  public void testGetSchemeOnly() {
    URIUtil.getScheme("kafka://");
  }

  @Test(expected = InvalidBeamURIException.class)
  public void testGetSchemeWithSingleSlash() {
    URIUtil.getScheme("kafka:/maypath/secondpath/thirdPath?format=avro");
  }

  @Test
  public void testGetQuery() {
    assertEquals(
        "format=avro", URIUtil.getQuery("kafka://maypath/secondpath/thirdPath?format=avro"));
  }

  @Test
  public void testGetQueryWithFullFilePathAndExtension() {
    assertEquals("format=zip", URIUtil.getQuery("file://maypath/secondpath/thirdPath/file.zip"));
  }

  @Test
  public void testGetQueryWithFullFilePathAndExtensionGz() {
    assertEquals("format=Gz", URIUtil.getQuery("file://maypath/secondpath/thirdPath/file.Gz"));
  }

  public void testGetQueryNullQuery() {
    URIUtil.getQuery("kafka://maypath/secondpath/thirdPath");
  }

  @Test(expected = InvalidBeamURIException.class)
  public void testGetQueryInvalidScheme() {
    URIUtil.getQuery("kafka://maypath/secondpath/thirdPath?");
  }

  @Test(expected = InvalidBeamURIException.class)
  public void testGetQueryInvalidQuery() {
    URIUtil.getQuery("kafka://maypath/secondpath/thirdPath?format");
  }

  @Test
  public void testGetPath() {
    assertEquals(
        "//maypath/secondpath/thirdPath/",
        URIUtil.getPath("kafka://maypath/secondpath/thirdPath/?format=gbp"));
  }

  @Test
  public void testBeamURIWithKafkaTopic() {
    BeamUri translatUri = URIUtil.translatUri("kafka://maypath?format=gbp");
    assertEquals("maypath", translatUri.getPath());
    assertEquals("format=gbp", translatUri.getFormat());
    assertEquals("kafka", translatUri.getScheme());
  }

  @Test(expected = InvalidBeamURIException.class)
  public void testBeamURIWithWrongKafkaURI() {
    BeamUri translatUri = URIUtil.translatUri("kafka://maypath/wong?format=gbp");
    assertEquals("maypath", translatUri.getPath());
    assertEquals("format=gbp", translatUri.getFormat());
    assertEquals("kafka", translatUri.getScheme());
  }

  @Test(expected = InvalidBeamURIException.class)
  public void testNoKafkaTopicURI() {
    BeamUri translatUri = URIUtil.translatUri("kafka://");
    assertEquals("maypath", translatUri.getPath());
    assertEquals("format=gbp", translatUri.getFormat());
    assertEquals("kafka", translatUri.getScheme());
  }

  @Test
  public void testmutlitpathKafkaTopicURI() {
    BeamUri translatUri = URIUtil.translatUri("kafka://topic1,topic2,topic3");
    assertEquals("topic1,topic2,topic3", translatUri.getPath());
  }

  @Test
  public void testBeamURIFileDirectory() {
    BeamUri translatUri = URIUtil.translatUri("file://maypath/secondpath/thirdPath/");
    assertEquals("//maypath/secondpath/thirdPath/", translatUri.getPath());
    assertEquals(null, translatUri.getFormat());
    assertEquals("file", translatUri.getScheme());
  }
}
