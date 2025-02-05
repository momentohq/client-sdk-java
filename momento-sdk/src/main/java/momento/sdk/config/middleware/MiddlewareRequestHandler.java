package momento.sdk.config.middleware;

import java.util.concurrent.CompletableFuture;

public interface MiddlewareRequestHandler {
  CompletableFuture<MiddlewareMetadata> onRequestMetadata(MiddlewareMetadata metadata);

  CompletableFuture<MiddlewareMessage> onRequestBody(MiddlewareMessage request);

  CompletableFuture<MiddlewareMetadata> onResponseMetadata(MiddlewareMetadata metadata);

  CompletableFuture<MiddlewareMessage> onResponseBody(MiddlewareMessage response);

  CompletableFuture<MiddlewareStatus> onResponseStatus(MiddlewareStatus status);
}
