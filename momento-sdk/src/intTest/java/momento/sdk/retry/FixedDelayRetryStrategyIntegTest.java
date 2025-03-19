package momento.sdk.retry;

import static momento.sdk.retry.BaseMomentoLocalTestClass.FIVE_SECONDS;
import static momento.sdk.retry.BaseMomentoLocalTestClass.withCacheAndCacheClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.IncrementResponse;
import momento.sdk.retry.utils.MomentoLocalMiddlewareArgs;
import momento.sdk.retry.utils.TestRetryMetricsCollector;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

public class FixedDelayRetryStrategyIntegTest {

  private static TestRetryMetricsCollector testRetryMetricsCollector;
  private static Logger logger;

  private static final Duration CLIENT_TIMEOUT_MILLIS = Duration.ofMillis(5000L);
  private static final int maxAttempts = 4;
  private static final long delayMillis = 1000L;
  private static final long maxDelayMillis = 4000L;

  @BeforeAll
  static void setup() {
    testRetryMetricsCollector = new TestRetryMetricsCollector();
    logger = getLogger(FixedDelayRetryStrategyIntegTest.class);
  }

  @Test
  void testRetryEligibleApi_shouldMakeMaxAttempts_WhenFullNetworkOutage() throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(FIVE_SECONDS)
              .asInstanceOf(InstanceOfAssertFactories.type(GetResponse.Error.class))
              .extracting(SdkException::getErrorCode)
              .isEqualTo(MomentoErrorCode.SERVER_UNAVAILABLE);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isGreaterThanOrEqualTo(4);
        });
  }

  @Test
  void testNonRetryEligibleApi_shouldMakeNoAttempts_WhenFullNetworkOutage() throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> false;

    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.INCREMENT))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.increment(cacheName, "key", 1))
              .succeedsWithin(FIVE_SECONDS)
              .asInstanceOf(InstanceOfAssertFactories.type(IncrementResponse.Error.class))
              .extracting(SdkException::getErrorCode)
              .isEqualTo(MomentoErrorCode.SERVER_UNAVAILABLE);

          assertThat(
                  testRetryMetricsCollector.getTotalRetryCount(
                      cacheName, MomentoRpcMethod.INCREMENT))
              .isEqualTo(0);
        });
  }

  @Test
  void testRetryEligibleApi_shouldMakeLessThanMaxAttempts_WhenTemporaryNetworkOutage()
      throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .errorCount(2)
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(FIVE_SECONDS)
              .isInstanceOf(GetResponse.Miss.class);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(2);
        });
  }

  @Test
  void
      testRetryEligibleApi_shouldMakeNoRetries_WhenTestConfigDelayMillisIsLessThanClientMaxDelayMillis()
          throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .delayMillis(500) // less than client max delay millis
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(FIVE_SECONDS)
              .isInstanceOf(GetResponse.Miss.class);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(0);
        });
  }

  @Test
  void
      testRetryEligibleApi_shouldMakeRetries_WhenTestConfigDelayMillisIsLessThanClientMaxDelayMillis()
          throws Exception {
    long maxDelayMillis = 2000L;
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .errorCount(2)
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .delayMillis(500) // less than client max delay millis
            .delayCount(2)
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(FIVE_SECONDS)
              .isInstanceOf(GetResponse.Miss.class);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(2);
        });
  }

  @Test
  void
      testRetryEligibleApi_shouldMakeNoRetries_WhenTestConfigDelayMillisIsGreaterThanClientMaxDelayMillis()
          throws Exception {
    long maxDelayMillis = 1000L;

    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .delayMillis(1500) // greater than client max delay millis
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(Duration.ofSeconds(10))
              .isInstanceOf(GetResponse.Miss.class);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(0);
        });
  }

  @Test
  void testRetryEligibleApi_shouldMakeMaxRetries_WhenDelayMillisIsLessThanMaxDelayMillis()
      throws Exception {
    long maxDelayMillis = 5000L;

    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(Duration.ofSeconds(10))
              .extracting(GetResponse::getClass)
              .isEqualTo(GetResponse.Error.class);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(4);
        });
  }
}
