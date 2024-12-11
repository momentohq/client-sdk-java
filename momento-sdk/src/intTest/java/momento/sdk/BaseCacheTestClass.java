package momento.sdk;

import java.time.Duration;
import java.util.UUID;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.Configurations;
import momento.sdk.config.ReadConcern;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseCacheTestClass {
  protected static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  protected static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
  protected static CredentialProvider credentialProvider;

  protected static CacheClient cacheClient;
  protected static CacheClient consistentReadCacheClient;
  protected static CacheClient balancedReadCacheClient;
  protected static String cacheName;

  @BeforeAll
  static void beforeAll() {
    final boolean consistentReads = System.getenv("CONSISTENT_READS") != null;

    credentialProvider = CredentialProvider.fromEnvVar("MOMENTO_API_KEY");

    final Configuration config = Configurations.Laptop.latest();
    final Configuration consistentConfig = config.withReadConcern(ReadConcern.CONSISTENT);

    balancedReadCacheClient =
        CacheClient.builder(credentialProvider, config, DEFAULT_TTL_SECONDS).build();
    consistentReadCacheClient =
        CacheClient.builder(credentialProvider, consistentConfig, DEFAULT_TTL_SECONDS).build();
    cacheClient = consistentReads ? consistentReadCacheClient : balancedReadCacheClient;

    cacheName = testCacheName();
    ensureTestCacheExists(cacheName);
  }

  @AfterAll
  static void afterAll() {
    cleanupTestCache(cacheName);
    balancedReadCacheClient.close();
    consistentReadCacheClient.close();
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
}
