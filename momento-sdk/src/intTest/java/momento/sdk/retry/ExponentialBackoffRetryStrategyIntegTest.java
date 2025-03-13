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
    final ExponentialBackoffRetryStrategy retryStrategy =
        new ExponentialBackoffRetryStrategy(100, 500);

    final MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) ->
            assertThat(cacheClient.get(cacheName, "key"))
                .succeedsWithin(FIVE_SECONDS)
                .asInstanceOf(InstanceOfAssertFactories.type(GetResponse.Error.class))
                .extracting(SdkException::getErrorCode)
                .isEqualTo(MomentoErrorCode.TIMEOUT_ERROR));
  }

  @Test
  void testNonRetryEligibleApi_shouldMakeNoAttempts_WhenFullNetworkOutage() throws Exception {
    final ExponentialBackoffRetryStrategy retryStrategy =
        new ExponentialBackoffRetryStrategy(100, 500);

    final MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
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
  void testRetryEligibleApi_shouldSucceed_WhenTemporaryNetworkOutage() throws Exception {
    final ExponentialBackoffRetryStrategy retryStrategy =
        new ExponentialBackoffRetryStrategy(100, 500);

    final MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
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
              .isGreaterThan(0);
        });
  }
}
