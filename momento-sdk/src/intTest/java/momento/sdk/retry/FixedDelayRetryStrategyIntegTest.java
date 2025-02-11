package momento.sdk.retry;

import static momento.sdk.retry.BaseCacheRetryTestClass.FIVE_SECONDS;
import static momento.sdk.retry.BaseCacheRetryTestClass.withCacheAndCacheClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.IncrementResponse;
import momento.sdk.retry.utils.TestRetryMetricsCollector;
import momento.sdk.retry.utils.TestRetryMetricsMiddlewareArgs;
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

    TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE.name())
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET.getRequestName()))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
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
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE.name())
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.INCREMENT.getRequestName()))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.increment(cacheName, "key", 1))
              .succeedsWithin(FIVE_SECONDS)
              .asInstanceOf(InstanceOfAssertFactories.type(IncrementResponse.Error.class))
              .extracting(SdkException::getErrorCode)
              .isEqualTo(MomentoErrorCode.SERVER_UNAVAILABLE);

          assertThat(
                  testRetryMetricsCollector.getTotalRetryCount(
                      cacheName, MomentoRpcMethod.INCREMENT))
              .isGreaterThanOrEqualTo(0);
        });
  }

  @Test
  void testNonRetryEligibleApi_shouldMakeLessThanMaxAttempts_WhenTemporaryNetworkOutage()
      throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE.name())
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET.getRequestName()))
            .errorCount(2)
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
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

    TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE.name())
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET.getRequestName()))
            .delayMillis(500) // less than client max delay millis
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
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

    TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE.name())
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET.getRequestName()))
            .errorCount(2)
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET.getRequestName()))
            .delayMillis(500) // less than client max delay millis
            .delayCount(2)
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
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

    TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET.getRequestName()))
            .delayMillis(1500) // greater than client max delay millis
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(FIVE_SECONDS)
              .isInstanceOf(GetResponse.Miss.class);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(0);
        });
  }
}
