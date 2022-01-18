package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.MomentoCacheResult;
import org.apache.commons.io.IOUtils;

final class ScsDataTestHelper {
  private ScsDataTestHelper() {}

  static void assertSetResponse(String expectedValue, CacheSetResponse setResponse)
      throws IOException {
    assertEquals(MomentoCacheResult.Ok, setResponse.result());
    assertEquals(expectedValue, setResponse.string().get());
    assertEquals(
        Arrays.toString(expectedValue.getBytes()), Arrays.toString(setResponse.byteArray().get()));
    assertEquals(ByteBuffer.wrap(expectedValue.getBytes()), setResponse.byteBuffer().get());
    assertTrue(
        IOUtils.contentEquals(
            IOUtils.toInputStream(expectedValue, StandardCharsets.UTF_8),
            setResponse.inputStream().get()));
  }
}
