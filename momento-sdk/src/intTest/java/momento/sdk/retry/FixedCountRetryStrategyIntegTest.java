package momento.sdk.retry;

import static momento.sdk.retry.BaseCacheRetryTestClass.withCacheAndCacheClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

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

public class FixedCountRetryStrategyIntegTest {

  private static TestRetryMetricsCollector testRetryMetricsCollector;
  private static Logger logger;

  @BeforeAll
  static void setup() {
    testRetryMetricsCollector = new TestRetryMetricsCollector();
    logger = getLogger(FixedCountRetryStrategyIntegTest.class);
  }

  @Test
  void testRetryEligibleApi_shouldMakeMaxAttempts_WhenFullNetworkOutage() throws Exception {
    TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE.name())
            .errorRpcList(List.of(MomentoRpcMethod.GET.getRequestName()))
            .build();

    withCacheAndCacheClient(
        config -> config,
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          GetResponse getResponse = cacheClient.get(cacheName, "key").join();
          assertEquals(GetResponse.Error.class, getResponse.getClass());
          assertEquals(
              MomentoErrorCode.SERVER_UNAVAILABLE,
              ((GetResponse.Error) getResponse).getErrorCode());

          int noOfRetries =
              testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET);
          assertEquals(3, noOfRetries);
        });
  }

  @Test
  void testNonRetryEligibleApi_shouldMakeNoAttempts_WhenFullNetworkOutage() throws Exception {
    TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE.name())
            .errorRpcList(List.of(MomentoRpcMethod.INCREMENT.getRequestName()))
            .build();

    withCacheAndCacheClient(
        config -> config,
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          IncrementResponse response = cacheClient.increment(cacheName, "key", 1).join();
          assertEquals(IncrementResponse.Error.class, response.getClass());
          assertEquals(
              MomentoErrorCode.SERVER_UNAVAILABLE,
              ((IncrementResponse.Error) response).getErrorCode());

          int noOfRetries =
              testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.INCREMENT);
          assertEquals(0, noOfRetries);
        });
  }

  @Test
  void testRetryEligibleApi_shouldMakeLessThanMaxAttempts_WhenTemporaryNetworkOutage()
      throws Exception {
    TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs =
        new TestRetryMetricsMiddlewareArgs.Builder(
                logger, testRetryMetricsCollector, UUID.randomUUID().toString())
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE.name())
            .errorRpcList(List.of(MomentoRpcMethod.GET.getRequestName()))
            .errorCount(2)
            .build();

    withCacheAndCacheClient(
        config -> config,
        testRetryMetricsMiddlewareArgs,
        (cacheClient, cacheName) -> {
          GetResponse response = cacheClient.get(cacheName, "key").join();
          assertEquals(GetResponse.Miss.class, response.getClass());
          int noOfRetries =
              testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET);
          assertThat(noOfRetries).isGreaterThan(1);
          assertThat(noOfRetries).isLessThanOrEqualTo(3);
        });
  }
}
