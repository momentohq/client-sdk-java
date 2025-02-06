package momento.sdk;

import io.grpc.Metadata;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import momento.sdk.config.middleware.MiddlewareMessage;
import momento.sdk.config.middleware.MiddlewareMetadata;
import momento.sdk.config.middleware.MiddlewareRequestHandler;
import momento.sdk.config.middleware.MiddlewareStatus;
import momento.sdk.exceptions.MomentoErrorCodeMetadataConverter;
import momento.sdk.retry.MomentoRpcMethod;
import momento.sdk.retry.MomentoRpcMethodMetadataConverter;

public class TestRetryMetricsMiddlewareRequestHandler implements MiddlewareRequestHandler {
  private String cacheName = null;
  private final TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs;

  public TestRetryMetricsMiddlewareRequestHandler(
      TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs) {
    this.testRetryMetricsMiddlewareArgs = testRetryMetricsMiddlewareArgs;
  }

  @Override
  public CompletableFuture<MiddlewareMetadata> onRequestMetadata(MiddlewareMetadata metadata) {
    Metadata grpcMetadata = metadata.getGrpcMetadata();
    setGrpcMetadata(grpcMetadata, "request-id", testRetryMetricsMiddlewareArgs.getRequestId());
    String returnError = testRetryMetricsMiddlewareArgs.getReturnError();
    if (returnError != null) {
      setGrpcMetadata(
          grpcMetadata, "return-error", MomentoErrorCodeMetadataConverter.convert(returnError));
    }
    String errorRpcMethods =
        (testRetryMetricsMiddlewareArgs.getErrorRpcList() != null)
            ? testRetryMetricsMiddlewareArgs.getErrorRpcList().stream()
                .map(
                    rpcMethod ->
                        MomentoRpcMethodMetadataConverter.convert(
                            MomentoRpcMethod.fromString(rpcMethod)))
                .collect(Collectors.joining(" "))
            : "";
    setGrpcMetadata(grpcMetadata, "error-rpcs", errorRpcMethods);
    String delayRpcMethods =
        (testRetryMetricsMiddlewareArgs.getDelayRpcList() != null)
            ? testRetryMetricsMiddlewareArgs.getDelayRpcList().stream()
                .map(
                    rpcMethod ->
                        MomentoRpcMethodMetadataConverter.convert(
                            MomentoRpcMethod.fromString(rpcMethod)))
                .collect(Collectors.joining(" "))
            : "";
    setGrpcMetadata(grpcMetadata, "delay-rpcs", delayRpcMethods);
    Integer errorCount = testRetryMetricsMiddlewareArgs.getErrorCount();
    if (errorCount != null) {
      setGrpcMetadata(grpcMetadata, "error-count", String.valueOf(errorCount));
    }
    Integer delayMillis = testRetryMetricsMiddlewareArgs.getDelayMillis();
    if (delayMillis != null) {
      setGrpcMetadata(grpcMetadata, "delay-ms", String.valueOf(delayMillis));
    }
    Integer delayCount = testRetryMetricsMiddlewareArgs.getDelayCount();
    if (delayCount != null) {
      setGrpcMetadata(grpcMetadata, "delay-count", String.valueOf(delayCount));
    }
    String cacheName = grpcMetadata.get(Metadata.Key.of("cache", Metadata.ASCII_STRING_MARSHALLER));
    if (cacheName != null) {
      this.cacheName = cacheName;
    } else {
      testRetryMetricsMiddlewareArgs.getLogger().debug("No cache name found in metadata.");
    }
    return CompletableFuture.completedFuture(metadata);
  }

  @Override
  public CompletableFuture<MiddlewareMessage> onRequestBody(MiddlewareMessage request) {
    String requestType = request.getConstructorName();

    if (cacheName != null) {
      testRetryMetricsMiddlewareArgs
          .getTestMetricsCollector()
          .addTimestamp(
              cacheName, MomentoRpcMethod.fromString(requestType), System.currentTimeMillis());
    } else {
      testRetryMetricsMiddlewareArgs
          .getLogger()
          .debug("No cache name available. Timestamp will not be collected.");
    }

    return CompletableFuture.completedFuture(request);
  }

  @Override
  public CompletableFuture<MiddlewareMetadata> onResponseMetadata(MiddlewareMetadata metadata) {
    return CompletableFuture.completedFuture(metadata);
  }

  @Override
  public CompletableFuture<MiddlewareMessage> onResponseBody(MiddlewareMessage response) {
    return CompletableFuture.completedFuture(response);
  }

  @Override
  public CompletableFuture<MiddlewareStatus> onResponseStatus(MiddlewareStatus status) {
    return CompletableFuture.completedFuture(status);
  }

  private void setGrpcMetadata(Metadata metadata, String key, String value) {
    if (value != null) {
      metadata.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);
    }
  }
}
