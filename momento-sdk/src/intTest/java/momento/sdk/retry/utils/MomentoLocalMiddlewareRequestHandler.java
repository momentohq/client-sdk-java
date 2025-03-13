package momento.sdk.retry.utils;

import io.grpc.Metadata;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import momento.sdk.config.middleware.MiddlewareMessage;
import momento.sdk.config.middleware.MiddlewareMetadata;
import momento.sdk.config.middleware.MiddlewareRequestHandler;
import momento.sdk.config.middleware.MiddlewareStatus;
import momento.sdk.exceptions.MomentoErrorCodeMetadataConverter;
import momento.sdk.retry.MomentoRpcMethod;
import momento.sdk.retry.MomentoRpcMethodMetadataConverter;

public class MomentoLocalMiddlewareRequestHandler implements MiddlewareRequestHandler {
  private String cacheName = null;
  private final MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs;

  public MomentoLocalMiddlewareRequestHandler(
      MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs) {
    this.momentoLocalMiddlewareArgs = momentoLocalMiddlewareArgs;
  }

  @Override
  public CompletableFuture<MiddlewareMetadata> onRequestMetadata(MiddlewareMetadata metadata) {
    final Metadata grpcMetadata = metadata.getGrpcMetadata();
    setGrpcMetadata(grpcMetadata, "request-id", momentoLocalMiddlewareArgs.getRequestId());

    momentoLocalMiddlewareArgs
        .getReturnError()
        .map(MomentoErrorCodeMetadataConverter::convert)
        .ifPresent(e -> setGrpcMetadata(grpcMetadata, "return-error", e));

    momentoLocalMiddlewareArgs
        .getErrorRpcList()
        .map(this::concatenateRPCs)
        .ifPresent(RPCs -> setGrpcMetadata(grpcMetadata, "error-rpcs", RPCs));

    momentoLocalMiddlewareArgs
        .getDelayRpcList()
        .map(this::concatenateRPCs)
        .ifPresent(RPCs -> setGrpcMetadata(grpcMetadata, "delay-rpcs", RPCs));

    momentoLocalMiddlewareArgs
        .getErrorCount()
        .map(String::valueOf)
        .ifPresent(errorCount -> setGrpcMetadata(grpcMetadata, "error-count", errorCount));

    momentoLocalMiddlewareArgs
        .getDelayMillis()
        .map(String::valueOf)
        .ifPresent(delayMillis -> setGrpcMetadata(grpcMetadata, "delay-ms", delayMillis));

    momentoLocalMiddlewareArgs
        .getDelayCount()
        .map(String::valueOf)
        .ifPresent(delayCount -> setGrpcMetadata(grpcMetadata, "delay-count", delayCount));

    momentoLocalMiddlewareArgs
        .getStreamErrorRpcList()
        .map(this::concatenateRPCs)
        .ifPresent(RPCs -> setGrpcMetadata(grpcMetadata, "stream-error-rpcs", RPCs));

    momentoLocalMiddlewareArgs
        .getStreamError()
        .map(MomentoErrorCodeMetadataConverter::convert)
        .ifPresent(e -> setGrpcMetadata(grpcMetadata, "stream-error", e));

    momentoLocalMiddlewareArgs
        .getStreamErrorMessageLimit()
        .map(String::valueOf)
        .ifPresent(limit -> setGrpcMetadata(grpcMetadata, "stream-error-message-limit", limit));

    final String cacheName =
        grpcMetadata.get(Metadata.Key.of("cache", Metadata.ASCII_STRING_MARSHALLER));
    if (cacheName != null) {
      this.cacheName = cacheName;
    } else {
      momentoLocalMiddlewareArgs.getLogger().debug("No cache name found in metadata.");
    }

    return CompletableFuture.completedFuture(metadata);
  }

  @Override
  public CompletableFuture<MiddlewareMessage> onRequestBody(MiddlewareMessage request) {
    final String requestType = request.getConstructorName();

    if (cacheName != null) {
      momentoLocalMiddlewareArgs
          .getTestMetricsCollector()
          .ifPresent(
              collector ->
                  collector.addTimestamp(
                      cacheName,
                      MomentoRpcMethod.fromString(requestType),
                      System.currentTimeMillis()));
    } else {
      momentoLocalMiddlewareArgs
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

  private String concatenateRPCs(List<MomentoRpcMethod> RPCs) {
    return RPCs.stream()
        .map(MomentoRpcMethodMetadataConverter::convert)
        .collect(Collectors.joining(" "));
  }
}
