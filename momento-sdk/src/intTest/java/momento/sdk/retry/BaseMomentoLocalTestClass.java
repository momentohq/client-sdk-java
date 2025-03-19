package momento.sdk.retry;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import momento.sdk.CacheClient;
import momento.sdk.TopicClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.MomentoLocalProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.Configurations;
import momento.sdk.config.TopicConfiguration;
import momento.sdk.config.TopicConfigurations;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.retry.utils.MomentoLocalMiddleware;
import momento.sdk.retry.utils.MomentoLocalMiddlewareArgs;
import momento.sdk.retry.utils.TestRetryMetricsCollector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;

public class BaseMomentoLocalTestClass {
  protected static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  protected static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
  protected static CacheClient cacheClient;
  protected static TestRetryMetricsCollector testRetryMetricsCollector;
  protected static MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs;
  protected static MomentoLocalMiddleware momentoLocalMiddleware;
  protected static Logger logger;

  protected final String hostname =
      Optional.ofNullable(System.getenv("MOMENTO_HOSTNAME")).orElse("127.0.0.1");
  protected final int port =
      Optional.ofNullable(System.getenv("MOMENTO_PORT")).map(Integer::parseInt).orElse(8080);

  @BeforeEach
  void beforeEach() {
    testRetryMetricsCollector = new TestRetryMetricsCollector();
    logger = getLogger(BaseMomentoLocalTestClass.class);
    final CredentialProvider credentialProvider = new MomentoLocalProvider(hostname, port);
    momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .build();
    momentoLocalMiddleware = new MomentoLocalMiddleware(momentoLocalMiddlewareArgs);
    final Configuration config =
        Configurations.Laptop.latest().withMiddleware(momentoLocalMiddleware);
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

  public static String testCacheName() {
    return "java-integration-test-default-" + UUID.randomUUID();
  }

  public static void withCacheAndCacheClient(
      Function<Configuration, Configuration> configFn,
      MomentoLocalMiddlewareArgs testMetricsMiddlewareArgs,
      CacheTestCallback testCallback)
      throws Exception {

    String cacheName = testCacheName();
    String hostname = Optional.ofNullable(System.getenv("MOMENTO_HOSTNAME")).orElse("127.0.0.1");
    int port =
        Optional.ofNullable(System.getenv("MOMENTO_PORT")).map(Integer::parseInt).orElse(8080);
    CredentialProvider credentialProvider = new MomentoLocalProvider(hostname, port);

    try (final CacheClient client =
        CacheClient.builder(
                credentialProvider,
                configFn
                    .apply(Configurations.Laptop.latest())
                    .withMiddleware(new MomentoLocalMiddleware(testMetricsMiddlewareArgs)),
                DEFAULT_TTL_SECONDS)
            .build()) {
      if (client.createCache(cacheName).join() instanceof CacheCreateResponse.Error) {
        throw new RuntimeException("Failed to create cache: " + cacheName);
      }
      testCallback.run(client, cacheName);
    }
  }

  public static void withCacheAndTopicClient(
      Function<TopicConfiguration, TopicConfiguration> configFn,
      MomentoLocalMiddlewareArgs testMetricsMiddlewareArgs,
      TopicTestCallback testCallback)
      throws Exception {

    final String cacheName = testCacheName();
    final String hostname =
        Optional.ofNullable(System.getenv("MOMENTO_HOSTNAME")).orElse("127.0.0.1");
    final int port =
        Optional.ofNullable(System.getenv("MOMENTO_PORT")).map(Integer::parseInt).orElse(8080);
    final CredentialProvider credentialProvider = new MomentoLocalProvider(hostname, port);

    try (final CacheClient cacheClient =
            CacheClient.builder(
                    credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
                .build();
        final TopicClient topicClient =
            TopicClient.builder(
                    credentialProvider,
                    configFn
                        .apply(TopicConfigurations.Laptop.latest())
                        .withMiddleware(new MomentoLocalMiddleware(testMetricsMiddlewareArgs)))
                .build()) {
      if (cacheClient.createCache(cacheName).join() instanceof CacheCreateResponse.Error) {
        throw new RuntimeException("Failed to create cache: " + cacheName);
      }
      testCallback.run(topicClient, cacheName);
    }
  }

  @FunctionalInterface
  public interface CacheTestCallback {
    void run(CacheClient cc, String cacheName) throws Exception;
  }

  @FunctionalInterface
  public interface TopicTestCallback {
    void run(TopicClient cc, String cacheName) throws Exception;
  }
}
