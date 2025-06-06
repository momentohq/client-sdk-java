package momento.sdk.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Metadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import momento.sdk.config.middleware.MiddlewareMetadata;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.exceptions.MomentoErrorCodeMetadataConverter;
import momento.sdk.retry.utils.MomentoLocalMiddlewareArgs;
import momento.sdk.retry.utils.MomentoLocalMiddlewareRequestHandler;
import org.junit.jupiter.api.Test;

public class MomentoLocalMiddlewareTest extends BaseMomentoLocalTestClass {

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
  }

  @Test
  public void shouldAddTimestampOnRequestBodyOnMultipleRequests() {
    String cacheName = testCacheName();
    ensureTestCacheExists(cacheName);
    cacheClient.set(cacheName, "key", "value").join();
    cacheClient.get(cacheName, "key").join();

    Map<String, Map<MomentoRpcMethod, List<Long>>> allMetrics =
        testRetryMetricsCollector.getAllMetrics();
    assertEquals(1, allMetrics.size());
    assertTrue(allMetrics.containsKey(cacheName));
    assertTrue(allMetrics.get(cacheName).containsKey(MomentoRpcMethod.SET));
    assertTrue(allMetrics.get(cacheName).containsKey(MomentoRpcMethod.GET));
    assertEquals(1, allMetrics.get(cacheName).get(MomentoRpcMethod.SET).size());
    assertEquals(1, allMetrics.get(cacheName).get(MomentoRpcMethod.GET).size());
  }

  @Test
  public void shouldAddMetadataToGrpcMetadata() {
    String requestId = UUID.randomUUID().toString();
    MomentoErrorCode returnError = MomentoErrorCode.SERVER_UNAVAILABLE;
    List<MomentoRpcMethod> errorRpcList = Collections.singletonList(MomentoRpcMethod.GET);
    int errorCount = 3;
    List<MomentoRpcMethod> delayRpcList = Collections.singletonList(MomentoRpcMethod.GET);
    int delayMillis = 100;
    int delayCount = 1;

    Metadata metadata = new Metadata();
    MiddlewareMetadata middlewareMetadata = new MiddlewareMetadata(metadata);
    MomentoLocalMiddlewareArgs args =
        new MomentoLocalMiddlewareArgs.Builder(logger, requestId)
            .testMetricsCollector(testRetryMetricsCollector)
            .returnError(returnError)
            .errorRpcList(errorRpcList)
            .errorCount(errorCount)
            .delayRpcList(delayRpcList)
            .delayMillis(delayMillis)
            .delayCount(delayCount)
            .build();

    MomentoLocalMiddlewareRequestHandler handler = new MomentoLocalMiddlewareRequestHandler(args);
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
