package momento.sdk;

import static momento.sdk.TestHelpers.DEFAULT_MOMENTO_HOSTED_ZONE_ENDPOINT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import momento.sdk.exceptions.CacheAlreadyExistsException;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.InvalidArgumentException;
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
    Momento momento = Momento.builder(authToken).build();
    runHappyPathTest(momento, cacheName);
  }

  @Test
  void recreatingCacheWithSameName_throwsAlreadyExists() {
    Momento momento = Momento.builder(authToken).build();
    momento.cacheBuilder(cacheName, 2).createCacheIfDoesntExist().build();
    assertThrows(CacheAlreadyExistsException.class, () -> momento.createCache(cacheName));
  }

  @Test
  void testInvalidCacheName() {
    Momento momento =
        Momento.builder(authToken).endpointOverride(DEFAULT_MOMENTO_HOSTED_ZONE_ENDPOINT).build();

    assertThrows(InvalidArgumentException.class, () -> momento.createCache("     "));
    assertThrows(
        InvalidArgumentException.class,
        () -> momento.cacheBuilder("     ", 2).createCacheIfDoesntExist().build());
  }

  @Test
  void clientWithEndpointOverride_succeeds() {
    Momento momentoWithEndpointOverride =
        Momento.builder(authToken).endpointOverride(DEFAULT_MOMENTO_HOSTED_ZONE_ENDPOINT).build();
    runHappyPathTest(momentoWithEndpointOverride, cacheName);
  }

  @Test
  void deleteCacheTest_succeeds() {
    String cacheName = "deleteCacheTest_succeeds-" + Math.random();
    Momento momento = Momento.builder(authToken).build();
    momento.createCache(cacheName);
    momento.cacheBuilder(cacheName, 2).build();
    momento.deleteCache(cacheName);
  }

  @Test
  void deleteForNonExistentCache_throwsNotFound() {
    String cacheName = "deleteCacheTest_failure-" + Math.random();
    Momento momento = Momento.builder(authToken).build();
    assertThrows(CacheNotFoundException.class, () -> momento.deleteCache(cacheName));
  }

  @Test
  void nonPositiveTtl_throwsException() {
    Momento momento = Momento.builder(authToken).build();
    assertThrows(ClientSdkException.class, () -> momento.cacheBuilder(cacheName, -1).build());
  }

  @Test
  void buildingCacheClientForNonExistentCache_throwsException() {
    Momento momento = Momento.builder(authToken).build();
    assertThrows(
        CacheNotFoundException.class,
        () -> momento.cacheBuilder(UUID.randomUUID().toString(), 60).build());
  }

  private static void runHappyPathTest(Momento momento, String cacheName) {
    Cache cache = momento.cacheBuilder(cacheName, 2).createCacheIfDoesntExist().build();

    String key = java.util.UUID.randomUUID().toString();

    // Set Key sync
    CacheSetResponse setRsp =
        cache.set(key, ByteBuffer.wrap("bar".getBytes(StandardCharsets.UTF_8)), 2);
    assertEquals(MomentoCacheResult.Ok, setRsp.result());

    // Get Key that was just set
    CacheGetResponse rsp = cache.get(key);
    assertEquals(MomentoCacheResult.Hit, rsp.result());
    assertEquals("bar", rsp.string().get());
  }
}
