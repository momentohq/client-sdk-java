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

  private static final Duration CLIENT_TIMEOUT_MILLIS = Duration.ofMillis(3000L);
  private static final long retryDelayIntervalMillis = 100;
  private static final long responseDataReceivedTimeoutMillis = 1000;

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
      testRetryEligibleApi_shouldTimeoutRetry_WhenRetryDelayIntervalMillisIsGreaterThanClientMaxDelayMillis()
          throws Exception {
    long retryDelayIntervalMillis = 4000;
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
              .isLessThanOrEqualTo(1);
        });
  }

  @Test
  void testRetryEligibleApi_shouldMakeNoRetries_WhenResponseDelayIsGreaterThanClientTimeout()
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
            .delayMillis(4000) // Delay greater than response data received timeout
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET))
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
  void testRetryEligibleApi_shouldRetry_WhenResponsesHaveShortDelaysDuringFullOutage()
      throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryDelayIntervalMillis, responseDataReceivedTimeoutMillis);

    int shortDelay = (int) (retryDelayIntervalMillis + 100);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .delayMillis(shortDelay)
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET))
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

          // Should receive errors after shortDelay ms and retry every RETRY_DELAY_INTERVAL_MILLIS
          // until the client timeout is reached.
          int delayBetweenAttempts = (int) (retryDelayIntervalMillis + shortDelay);
          int maxAttempts =
              (int) Math.round(CLIENT_TIMEOUT_MILLIS.toMillis() / (double) delayBetweenAttempts);
          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isBetween(2, maxAttempts);

          // Jitter will be +/- 10% of the delay between retry attempts
          double maxDelay = delayBetweenAttempts * 1.1;
          double minDelay = delayBetweenAttempts * 0.9;
          double average =
              testRetryMetricsCollector.getAverageTimeBetweenRetries(
                  cacheName, MomentoRpcMethod.GET);
          assertThat(average).isBetween(minDelay, maxDelay);
        });
  }

  @Test
  void testRetryEligibleApi_shouldRetry_WhenResponsesHaveLongDelaysDuringFullOutage()
      throws Exception {
    RetryEligibilityStrategy eligibilityStrategy = (status, methodName) -> true;

    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryDelayIntervalMillis, responseDataReceivedTimeoutMillis);

    // Momento-local should delay responses for longer than the retry timeout so that
    // we can test the retry strategy's timeout is actually being respected.
    int longDelay = (int) (responseDataReceivedTimeoutMillis + 500);

    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .delayMillis(longDelay)
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .build();

    Duration clientTimeout = Duration.ofMillis(6000L);
    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(clientTimeout),
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(Duration.ofSeconds(8))
              .asInstanceOf(InstanceOfAssertFactories.type(GetResponse.Error.class))
              .extracting(SdkException::getErrorCode)
              .isEqualTo(MomentoErrorCode.TIMEOUT_ERROR);

          // Should receive errors after longDelay ms and retry every RETRY_DELAY_INTERVAL_MILLIS
          // until the client timeout is reached.
          int delayBetweenAttempts = (int) (retryDelayIntervalMillis + longDelay);
          int maxAttempts =
              (int) Math.ceil(clientTimeout.toMillis() / (double) delayBetweenAttempts) + 1;
          // Fixed timeout retry strategy should retry at least twice.
          // If it retries only once, it could mean that the retry attempt is timing out and if we
          // aren't
          // handling that case correctly, then it won't continue retrying until the client timeout
          // is reached.
          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isBetween(2, maxAttempts);

          // Jitter will contribute +/- 10% of the delay between retry attempts, and estimating
          // the request will take up to 10% more time as well.
          // The expected delay here is not longDelay because the retry strategy's timeout is
          // shorter than that and retry attempts should stop before longDelay is reached.
          double expectedDelayBetweenAttempts =
              responseDataReceivedTimeoutMillis + retryDelayIntervalMillis;
          double maxDelay = expectedDelayBetweenAttempts * 1.2;
          double minDelay = expectedDelayBetweenAttempts * 0.9;
          double average =
              testRetryMetricsCollector.getAverageTimeBetweenRetries(
                  cacheName, MomentoRpcMethod.GET);
          assertThat(average).isBetween(minDelay, maxDelay);
        });
  }
}
