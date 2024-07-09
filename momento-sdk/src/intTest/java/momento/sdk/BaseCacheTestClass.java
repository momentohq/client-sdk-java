package momento.sdk;

import java.time.Duration;
import java.util.UUID;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseCacheTestClass {
  protected static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  protected static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
  protected static CredentialProvider credentialProvider;

  protected static CacheClient cacheClient;
  protected static String cacheName;

  @BeforeAll
  static void beforeAll() {
    credentialProvider = CredentialProvider.fromEnvVar("TEST_AUTH_TOKEN");
    cacheClient =
        CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
            .build();
    cacheName = testCacheName();
    ensureTestCacheExists(cacheName);
  }

  @AfterAll
  static void afterAll() {
    cleanupTestCache(cacheName);
    cacheClient.close();
  }

  protected static void ensureTestCacheExists(String cacheName) {
    CacheCreateResponse response = cacheClient.createCache(cacheName).join();
    if (response instanceof CacheCreateResponse.Error) {
      throw new RuntimeException(
          "Failed to test create cache: " + ((CacheCreateResponse.Error) response).getMessage());
    }
  }

  public static void cleanupTestCache(String cacheName) {
    CacheDeleteResponse response = cacheClient.deleteCache(cacheName).join();
    if (response instanceof CacheDeleteResponse.Error) {
      throw new RuntimeException(
          "Failed to test delete cache: " + ((CacheDeleteResponse.Error) response).getMessage());
    }
  }

  public static String testCacheName() {
    return "java-integration-test-default-" + UUID.randomUUID();
  }

  public static String testStoreName() {
    return "java-integration-test-default-" + UUID.randomUUID();
  }
}
