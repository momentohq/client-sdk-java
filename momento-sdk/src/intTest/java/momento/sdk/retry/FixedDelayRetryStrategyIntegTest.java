package momento.sdk.retry;

import static momento.sdk.retry.BaseCacheRetryTestClass.withCacheAndCacheClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.IncrementResponse;
import momento.sdk.retry.utils.TestRetryMetricsCollector;
import momento.sdk.retry.utils.TestRetryMetricsMiddlewareArgs;
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
            .errorRpcList(List.of(MomentoRpcMethod.GET.getRequestName()))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          GetResponse getResponse = cacheClient.get(cacheName, "key").join();
          assertEquals(GetResponse.Error.class, getResponse.getClass());
          assertEquals(
              MomentoErrorCode.SERVER_UNAVAILABLE,
              ((GetResponse.Error) getResponse).getErrorCode());
          int noOfRetries =
              testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET);
          assertThat(noOfRetries).isGreaterThanOrEqualTo(4);
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
            .errorRpcList(List.of(MomentoRpcMethod.INCREMENT.getRequestName()))
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          IncrementResponse response = cacheClient.increment(cacheName, "key", 1).join();
          assertEquals(IncrementResponse.Error.class, response.getClass());
          assertEquals(
              MomentoErrorCode.SERVER_UNAVAILABLE,
              ((IncrementResponse.Error) response).getErrorCode());
          int noOfRetries =
              testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.INCREMENT);
          assertThat(noOfRetries).isGreaterThanOrEqualTo(0);
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
            .errorRpcList(List.of(MomentoRpcMethod.GET.getRequestName()))
            .errorCount(2)
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          GetResponse response = cacheClient.get(cacheName, "key").join();
          assertEquals(GetResponse.Miss.class, response.getClass());
          int noOfRetries =
              testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET);
          assertEquals(2, noOfRetries);
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
            .delayRpcList(List.of(MomentoRpcMethod.GET.getRequestName()))
            .delayMillis(500) // less than client max delay millis
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          GetResponse response = cacheClient.get(cacheName, "key").join();
          assertEquals(GetResponse.Miss.class, response.getClass());
          int noOfRetries =
              testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET);
          assertEquals(0, noOfRetries);
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
            .errorRpcList(List.of(MomentoRpcMethod.GET.getRequestName()))
            .errorCount(2)
            .delayRpcList(List.of(MomentoRpcMethod.GET.getRequestName()))
            .delayMillis(500) // less than client max delay millis
            .delayCount(2)
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          GetResponse response = cacheClient.get(cacheName, "key").join();
          assertEquals(GetResponse.Miss.class, response.getClass());
          int noOfRetries =
              testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET);
          assertEquals(2, noOfRetries);
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
            .delayRpcList(List.of(MomentoRpcMethod.GET.getRequestName()))
            .delayMillis(1500) // greater than client max delay millis
            .build();

    withCacheAndCacheClient(
        config -> config.withRetryStrategy(retryStrategy).withTimeout(CLIENT_TIMEOUT_MILLIS),
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          GetResponse response = cacheClient.get(cacheName, "key").join();
          assertEquals(GetResponse.Miss.class, response.getClass());
          int noOfRetries =
              testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET);
          assertEquals(0, noOfRetries);
        });
  }
}
