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

public class FixedTimeoutRetryStrategyIntegTest {
  private static TestRetryMetricsCollector testRetryMetricsCollector;
  private static Logger logger;

  private static final Duration CLIENT_TIMEOUT_MILLIS = Duration.ofMillis(5000L);
  private static final long retryDelayIntervalMillis = 1000;
  private static final long responseDataReceivedTimeoutMillis = 4000;

  @BeforeAll
  public static void setUp() {
    testRetryMetricsCollector = new TestRetryMetricsCollector();
    logger = getLogger(FixedTimeoutRetryStrategyIntegTest.class);
  }

  @Test
  void testRetryEligibleApi_shouldMakeMaxAttempts_WhenFullNetworkOutage() throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryDelayIntervalMillis, responseDataReceivedTimeoutMillis);

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
              .isEqualTo(MomentoErrorCode.TIMEOUT_ERROR);

          long maxRetries = CLIENT_TIMEOUT_MILLIS.toMillis() / retryDelayIntervalMillis;

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isGreaterThanOrEqualTo(1) // At least one retry
              .isLessThanOrEqualTo((int) maxRetries);
        });
  }

  @Test
  void testNonRetryEligibleApi_shouldMakeNoAttempts_WhenFullNetworkOutage() throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> false;

    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryDelayIntervalMillis, responseDataReceivedTimeoutMillis);

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

    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryDelayIntervalMillis, responseDataReceivedTimeoutMillis);

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
      testRetryEligibleApi_shouldMakeNoRetries_WhenTestRetryDelayIntervalMillisIsGreaterThanClientMaxDelayMillis()
          throws Exception {
    long retryDelayIntervalMillis = 6000;
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryDelayIntervalMillis, responseDataReceivedTimeoutMillis);

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
              .isEqualTo(MomentoErrorCode.TIMEOUT_ERROR);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(0);
        });
  }

  @Test
  void
      testRetryEligibleApi_shouldMakeNoRetries_WhenTestConfigDelayMillisIsGreaterThanResponseDataReceivedTimeoutMillis()
          throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryDelayIntervalMillis, responseDataReceivedTimeoutMillis);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .delayMillis(5000) // Delay greater than response data received timeout
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(Duration.ofSeconds(10))
              .asInstanceOf(InstanceOfAssertFactories.type(GetResponse.Error.class))
              .extracting(SdkException::getErrorCode)
              .isEqualTo(MomentoErrorCode.TIMEOUT_ERROR);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(0);
        });
  }

  @Test
  void
      testRetryEligibleApi_shouldMakeNoRetries_WhenTestConfigDelayMillisIsGreaterThanClientTimeoutMillis()
          throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryDelayIntervalMillis, responseDataReceivedTimeoutMillis);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .delayMillis(6000) // Delay greater than client timeout
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(Duration.ofSeconds(10))
              .asInstanceOf(InstanceOfAssertFactories.type(GetResponse.Error.class))
              .extracting(SdkException::getErrorCode)
              .isEqualTo(MomentoErrorCode.TIMEOUT_ERROR);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(0);
        });
  }

  @Test
  void
      testRetryEligibleApi_shouldRetry_WhenTestConfigDelayMillisIsLessThanResponseDataReceivedTimeoutMillis()
          throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryDelayIntervalMillis, responseDataReceivedTimeoutMillis);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .errorCount(1) // return error only on first attempt
            .delayMillis(100) // Delay less than response data received timeout
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .delayCount(1) // Delay only on first attempt
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(FIVE_SECONDS)
              .isInstanceOf(GetResponse.Miss.class);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(1);
        });
  }
}
