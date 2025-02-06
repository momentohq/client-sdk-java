package momento.sdk.retry;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.UUID;
import momento.sdk.CacheClient;
import momento.sdk.TestRetryMetricsCollector;
import momento.sdk.TestRetryMetricsMiddleware;
import momento.sdk.TestRetryMetricsMiddlewareArgs;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;

public class BaseCacheRetryTestClass {
  protected static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  protected static CacheClient cacheClient;
  protected static CredentialProvider credentialProvider;
  protected static TestRetryMetricsCollector testRetryMetricsCollector;
  protected static TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs;
  protected static TestRetryMetricsMiddleware testRetryMetricsMiddleware;
  protected static Logger logger;

  @BeforeEach
  void beforeEach() {
    testRetryMetricsCollector = new TestRetryMetricsCollector();
    logger = getLogger(BaseCacheRetryTestClass.class);
    credentialProvider = CredentialProvider.fromEnvVar("MOMENTO_API_KEY");
    testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .build();
    testRetryMetricsMiddleware = new TestRetryMetricsMiddleware(testRetryMetricsMiddlewareArgs);
    final Configuration config =
        Configurations.Laptop.latest().withMiddleware(testRetryMetricsMiddleware);
    cacheClient = CacheClient.builder(credentialProvider, config, DEFAULT_TTL_SECONDS).build();
  }

  @AfterEach
  void afterEach() {
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
}
