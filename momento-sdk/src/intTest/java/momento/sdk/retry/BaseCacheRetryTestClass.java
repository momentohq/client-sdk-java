package momento.sdk.retry;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.MomentoLocalProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import momento.sdk.retry.utils.TestRetryMetricsCollector;
import momento.sdk.retry.utils.TestRetryMetricsMiddleware;
import momento.sdk.retry.utils.TestRetryMetricsMiddlewareArgs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;

public class BaseCacheRetryTestClass {
  protected static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  protected static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
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

  public static void withCacheAndCacheClient(
      Function<Configuration, Configuration> configFn,
      TestRetryMetricsMiddlewareArgs testMetricsMiddlewareArgs,
      CacheTestCallback testCallback)
      throws Exception {

    String cacheName = testCacheName();
    String hostname = Optional.ofNullable(System.getenv("MOMENTO_HOSTNAME")).orElse("127.0.0.1");
    int port = Integer.parseInt(Optional.ofNullable(System.getenv("MOMENTO_PORT")).orElse("8080"));
    CredentialProvider credentialProvider = new MomentoLocalProvider(hostname, port);

    CacheClient client =
        CacheClient.builder(
                credentialProvider,
                configFn
                    .apply(Configurations.Laptop.latest())
                    .withMiddleware(new TestRetryMetricsMiddleware(testMetricsMiddlewareArgs)),
                DEFAULT_TTL_SECONDS)
            .build();

    try {
      if (client.createCache(cacheName).join() instanceof CacheCreateResponse.Error) {
        throw new RuntimeException("Failed to create cache: " + cacheName);
      }
      testCallback.run(client, cacheName);
    } finally {
      client.deleteCache(cacheName).join(); // Cleanup cache
      client.close(); // Close the client
    }
  }

  @FunctionalInterface
  public interface CacheTestCallback {
    void run(CacheClient cc, String cacheName) throws Exception;
  }
}
