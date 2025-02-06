package momento.sdk.config.middleware;

public interface Middleware {
  MiddlewareRequestHandler onNewRequest(MiddlewareRequestHandlerContext context);

  default void close() {}
}
