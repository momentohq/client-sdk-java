package momento.sdk.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Metadata;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import momento.sdk.config.middleware.MiddlewareMetadata;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.exceptions.MomentoErrorCodeMetadataConverter;
import momento.sdk.retry.utils.TestRetryMetricsMiddlewareArgs;
import momento.sdk.retry.utils.TestRetryMetricsMiddlewareRequestHandler;
import org.junit.jupiter.api.Test;

public class TestRetryMetricsMiddlewareTest extends BaseCacheRetryTestClass {

  @Test
  public void shouldAddTimestampOnRequestBodyOnSingleCache() {
    String cacheName = testCacheName();
    ensureTestCacheExists(cacheName);
    cacheClient.get(cacheName, "key").join();

    Map<String, Map<MomentoRpcMethod, List<Long>>> allMetrics =
        testRetryMetricsCollector.getAllMetrics();
    assertEquals(1, allMetrics.size());
    assertTrue(allMetrics.containsKey(cacheName));
    assertTrue(allMetrics.get(cacheName).containsKey(MomentoRpcMethod.GET));
    assertEquals(1, allMetrics.get(cacheName).get(MomentoRpcMethod.GET).size());
    cleanupTestCache(cacheName);
  }

  @Test
  public void shouldAddTimestampOnRequestBodyOnMultipleCaches() {
    String cacheName1 = testCacheName();
    String cacheName2 = testCacheName();
    ensureTestCacheExists(cacheName1);
    ensureTestCacheExists(cacheName2);
    cacheClient.get(cacheName1, "key").join();
    cacheClient.get(cacheName2, "key").join();

    Map<String, Map<MomentoRpcMethod, List<Long>>> allMetrics =
        testRetryMetricsCollector.getAllMetrics();
    assertEquals(2, allMetrics.size());
    assertTrue(allMetrics.containsKey(cacheName1));
    assertTrue(allMetrics.containsKey(cacheName2));
    assertTrue(allMetrics.get(cacheName1).containsKey(MomentoRpcMethod.GET));
    assertTrue(allMetrics.get(cacheName2).containsKey(MomentoRpcMethod.GET));
    assertEquals(1, allMetrics.get(cacheName1).get(MomentoRpcMethod.GET).size());
    assertEquals(1, allMetrics.get(cacheName2).get(MomentoRpcMethod.GET).size());
    cleanupTestCache(cacheName1);
    cleanupTestCache(cacheName2);
  }

  @Test
  public void shouldAddTimestampOnRequestBodyOnMultipleRequests() {
    String cacheName = testCacheName();
    ensureTestCacheExists(cacheName);
    cacheClient.set(cacheName, "key", "value").join();
    cacheClient.get(cacheName, "key").join();

    Map<String, Map<MomentoRpcMethod, List<Long>>> allMetrics =
        testRetryMetricsCollector.getAllMetrics();

    System.out.println(allMetrics);

    assertEquals(1, allMetrics.size());
    assertTrue(allMetrics.containsKey(cacheName));
    assertTrue(allMetrics.get(cacheName).containsKey(MomentoRpcMethod.SET));
    assertTrue(allMetrics.get(cacheName).containsKey(MomentoRpcMethod.GET));
    assertEquals(1, allMetrics.get(cacheName).get(MomentoRpcMethod.SET).size());
    assertEquals(1, allMetrics.get(cacheName).get(MomentoRpcMethod.GET).size());
    cleanupTestCache(cacheName);
  }

  @Test
  public void shouldAddMetadataToGrpcMetadata() {
    String requestId = UUID.randomUUID().toString();
    String returnError = MomentoErrorCode.SERVER_UNAVAILABLE.name();
    List<String> errorRpcList = List.of(MomentoRpcMethod.GET.getRequestName());
    System.out.println(errorRpcList);
    int errorCount = 3;
    List<String> delayRpcList = List.of(MomentoRpcMethod.GET.getRequestName());
    int delayMillis = 100;
    int delayCount = 1;

    Metadata metadata = new Metadata();
    MiddlewareMetadata middlewareMetadata = new MiddlewareMetadata(metadata);
    TestRetryMetricsMiddlewareArgs args =
        new TestRetryMetricsMiddlewareArgs.Builder(logger, testRetryMetricsCollector, requestId)
            .returnError(returnError)
            .errorRpcList(errorRpcList)
            .errorCount(errorCount)
            .delayRpcList(delayRpcList)
            .delayMillis(delayMillis)
            .delayCount(delayCount)
            .build();

    TestRetryMetricsMiddlewareRequestHandler handler =
        new TestRetryMetricsMiddlewareRequestHandler(args);
    handler.onRequestMetadata(middlewareMetadata);

    String expectedRpcList = MomentoRpcMethodMetadataConverter.convert(MomentoRpcMethod.GET);
    assertEquals(
        requestId, metadata.get(Metadata.Key.of("request-id", Metadata.ASCII_STRING_MARSHALLER)));
    assertEquals(
        MomentoErrorCodeMetadataConverter.convert(returnError),
        metadata.get(Metadata.Key.of("return-error", Metadata.ASCII_STRING_MARSHALLER)));
    assertEquals(
        expectedRpcList,
        metadata.get(Metadata.Key.of("error-rpcs", Metadata.ASCII_STRING_MARSHALLER)));
    assertEquals(
        String.valueOf(errorCount),
        metadata.get(Metadata.Key.of("error-count", Metadata.ASCII_STRING_MARSHALLER)));
    assertEquals(
        expectedRpcList,
        metadata.get(Metadata.Key.of("delay-rpcs", Metadata.ASCII_STRING_MARSHALLER)));
    assertEquals(
        String.valueOf(delayMillis),
        metadata.get(Metadata.Key.of("delay-ms", Metadata.ASCII_STRING_MARSHALLER)));
    assertEquals(
        String.valueOf(delayCount),
        metadata.get(Metadata.Key.of("delay-count", Metadata.ASCII_STRING_MARSHALLER)));
  }
}
