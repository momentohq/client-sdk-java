package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import momento.sdk.messages.CacheSetResponse;
import org.apache.commons.io.IOUtils;

final class ScsDataTestHelper {
  private ScsDataTestHelper() {}

  static void assertSetResponse(String expectedValue, CacheSetResponse setResponse)
      throws IOException {
    assertEquals(expectedValue, setResponse.string());
    assertArrayEquals(expectedValue.getBytes(), setResponse.byteArray());
    assertEquals(ByteBuffer.wrap(expectedValue.getBytes()), setResponse.byteBuffer());
    assertTrue(
        IOUtils.contentEquals(
            IOUtils.toInputStream(expectedValue, StandardCharsets.UTF_8),
            setResponse.inputStream()));
  }
}
