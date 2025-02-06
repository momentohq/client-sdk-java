package momento.sdk;

import java.util.concurrent.CompletableFuture;
import momento.sdk.config.middleware.*;

public class MyCustomMiddleware implements Middleware {

  @Override
  public MiddlewareRequestHandler onNewRequest(MiddlewareRequestHandlerContext context) {
    return new MiddlewareRequestHandler() {
      @Override
      public CompletableFuture<MiddlewareMetadata> onRequestMetadata(MiddlewareMetadata metadata) {
        System.out.println("Request metadata: " + metadata.getGrpcMetadata());
        return CompletableFuture.completedFuture(metadata);
      }

      @Override
      public CompletableFuture<MiddlewareMessage> onRequestBody(MiddlewareMessage request) {
        System.out.println("Request body: " + request.getMessage());
        return CompletableFuture.completedFuture(request);
      }

      @Override
      public CompletableFuture<MiddlewareMetadata> onResponseMetadata(MiddlewareMetadata metadata) {
        System.out.println("Response metadata: " + metadata.getGrpcMetadata());
        return CompletableFuture.completedFuture(metadata);
      }

      @Override
      public CompletableFuture<MiddlewareMessage> onResponseBody(MiddlewareMessage response) {
        System.out.println("Response body: " + response.getMessage());
        return CompletableFuture.completedFuture(response);
      }

      @Override
      public CompletableFuture<MiddlewareStatus> onResponseStatus(MiddlewareStatus status) {
        System.out.println("Response status: " + status.getGrpcStatus());
        return CompletableFuture.completedFuture(status);
      }
    };
  }
}
