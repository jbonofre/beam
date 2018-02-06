package org.apache.beam.sdk.extensions.dsls.xml.flow.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
/**
 * A test case for the {@link BeamUri} class.
 *
 */
public class BeamURITest {

  @Test
  public void testBeamURI() {
    BeamUri translatUri = URIUtil.translatUri("file://tmp/location/file.csv?format=csv");
    assertEquals("file", translatUri.getScheme());
    assertEquals("//tmp/location/file.csv", translatUri.getPath());
    assertEquals("format=csv", translatUri.getFormat());
    assertNotNull(translatUri);
  }

}
