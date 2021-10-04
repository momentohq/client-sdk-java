package momento.sdk;

import static momento.sdk.TestHelpers.DEFAULT_MOMENTO_HOSTED_ZONE_ENDPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import momento.sdk.exceptions.CacheAlreadyExistsException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.MomentoCacheResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class MomentoTest {

  private String authToken;
  private String cacheName;

  @BeforeAll
  static void beforeAll() {
    if (System.getenv("TEST_AUTH_TOKEN") == null) {
      throw new IllegalArgumentException(
          "Integration tests require TEST_AUTH_TOKEN env var; see README for more details.");
    }
    if (System.getenv("TEST_CACHE_NAME") == null) {
      throw new IllegalArgumentException(
          "Integration tests require TEST_CACHE_NAME env var; see README for more details.");
    }
  }

  @BeforeEach
  void setup() {
    this.authToken = System.getenv("TEST_AUTH_TOKEN");
    this.cacheName = System.getenv("TEST_CACHE_NAME");
  }

  @Test
  void testHappyPath() {
    Momento momento =
        Momento.builder()
            .authToken(authToken)
            .endpointOverride(DEFAULT_MOMENTO_HOSTED_ZONE_ENDPOINT)
            .build();
    Cache cache = getOrCreate(momento, cacheName);

    String key = java.util.UUID.randomUUID().toString();

    // Set Key sync
    CacheSetResponse setRsp =
        cache.set(key, ByteBuffer.wrap("bar".getBytes(StandardCharsets.UTF_8)), 2);
    assertEquals(MomentoCacheResult.Ok, setRsp.result());

    // Get Key that was just set
    CacheGetResponse rsp = cache.get(key);
    assertEquals(MomentoCacheResult.Hit, rsp.result());
    assertEquals("bar", rsp.asStringUtf8().get());
  }

  // TODO: Update this to be recreated each time and add a separate test case for Already Exists
  private static Cache getOrCreate(Momento momento, String cacheName) {
    try {
      return momento.createCache(cacheName).cache();
    } catch (CacheAlreadyExistsException e) {
      return momento.getCache(cacheName);
    }
  }
}
