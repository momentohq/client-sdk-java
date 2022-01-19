package momento.sdk;

import momento.sdk.exceptions.CacheAlreadyExistsException;
import org.junit.jupiter.api.BeforeAll;

class BaseTestClass {

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
    ensureTestCacheExists();
  }

  private static void ensureTestCacheExists() {
    SimpleCacheClient client =
        SimpleCacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), 10).build();
    try {
      client.createCache(System.getenv("TEST_CACHE_NAME"));
    } catch (CacheAlreadyExistsException e) {
      // do nothing. Cache already exists.
    } finally {
      client.close();
    }
  }
}
