package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import momento.sdk.exceptions.ValidationException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.MomentoCacheResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Just includes a happy test path that interacts with both control and data plane clients. */
final class SimpleCacheClientTest extends BaseTestClass {

  private static final int DEFAULT_TTL_SECONDS = 60;

  private SimpleCacheClient target;

  @BeforeEach
  void setup() {
    target =
        SimpleCacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), DEFAULT_TTL_SECONDS).build();
  }

  @AfterEach
  void teardown() {
    target.close();
  }

  @Test
  public void createCacheGetSetValuesAndDeleteCache() {
    String cacheName = UUID.randomUUID().toString();
    String key = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    target.createCache(cacheName);
    CacheSetResponse response = target.set(cacheName, key, value);

    CacheGetResponse getResponse = target.get(cacheName, key);
    assertEquals(MomentoCacheResult.Hit, getResponse.result());
    assertEquals(value, getResponse.string().get());

    CacheGetResponse getForKeyInSomeOtherCache = target.get(System.getenv("TEST_CACHE_NAME"), key);
    assertEquals(MomentoCacheResult.Miss, getForKeyInSomeOtherCache.result());

    target.deleteCache(cacheName);
  }

  @Test
  public void throwsExceptionWhenClientUsesNegativeDefaultTtl() {
    assertThrows(
        ValidationException.class,
        () -> SimpleCacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), -1).build());
  }
}
