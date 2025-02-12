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

public class ExponentialBackoffRetryStrategyIntegTest {

  private static TestRetryMetricsCollector testRetryMetricsCollector;
  private static Logger logger;

  private static final Duration CLIENT_TIMEOUT_MILLIS = Duration.ofMillis(2000L);

  @BeforeAll
  static void setup() {
    testRetryMetricsCollector = new TestRetryMetricsCollector();
    logger = getLogger(ExponentialBackoffRetryStrategyIntegTest.class);
  }

  @Test
  void testRetryEligibleApi_shouldHitDeadline_WhenFullNetworkOutage() throws Exception {
    final RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    final ExponentialBackoffRetryStrategy retryStrategy =
        new ExponentialBackoffRetryStrategy(100, 500, eligibilityStrategy);

    final TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE.name())
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET.getRequestName()))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) ->
            assertThat(cacheClient.get(cacheName, "key"))
                .succeedsWithin(FIVE_SECONDS)
                .asInstanceOf(InstanceOfAssertFactories.type(GetResponse.Error.class))
                .extracting(SdkException::getErrorCode)
                .isEqualTo(MomentoErrorCode.TIMEOUT_ERROR));
  }

  @Test
  void testNonRetryEligibleApi_shouldMakeNoAttempts_WhenFullNetworkOutage() throws Exception {
    final RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> false;

    final ExponentialBackoffRetryStrategy retryStrategy =
        new ExponentialBackoffRetryStrategy(100, 500, eligibilityStrategy);

    final TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
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
              .isEqualTo(0);
        });
  }
}
