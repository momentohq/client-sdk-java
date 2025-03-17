package momento.sdk.retry;

import static momento.sdk.retry.BaseMomentoLocalTestClass.FIVE_SECONDS;
import static momento.sdk.retry.BaseMomentoLocalTestClass.withCacheAndCacheClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.LoggerFactory.getLogger;

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
    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .build();

    withCacheAndCacheClient(
        config -> config,
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(FIVE_SECONDS)
              .asInstanceOf(InstanceOfAssertFactories.type(GetResponse.Error.class))
              .extracting(SdkException::getErrorCode)
              .isEqualTo(MomentoErrorCode.SERVER_UNAVAILABLE);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isEqualTo(3);
        });
  }

  @Test
  void testNonRetryEligibleApi_shouldMakeNoAttempts_WhenFullNetworkOutage() throws Exception {
    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.INCREMENT))
            .build();

    withCacheAndCacheClient(
        config -> config,
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
    MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .errorRpcList(Collections.singletonList(MomentoRpcMethod.GET))
            .errorCount(2)
            .build();

    withCacheAndCacheClient(
        config -> config,
        momentoLocalMiddlewareArgs,
        (cacheClient, cacheName) -> {
          assertThat(cacheClient.get(cacheName, "key"))
              .succeedsWithin(FIVE_SECONDS)
              .isInstanceOf(GetResponse.Miss.class);

          assertThat(testRetryMetricsCollector.getTotalRetryCount(cacheName, MomentoRpcMethod.GET))
              .isBetween(2, 3);
        });
  }
}
