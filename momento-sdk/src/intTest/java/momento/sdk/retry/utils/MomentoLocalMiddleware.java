package momento.sdk.retry.utils;

import momento.sdk.config.middleware.Middleware;
import momento.sdk.config.middleware.MiddlewareRequestHandler;
import momento.sdk.config.middleware.MiddlewareRequestHandlerContext;

public class MomentoLocalMiddleware implements Middleware {
  private final MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs;

  public MomentoLocalMiddleware(MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs) {
    this.momentoLocalMiddlewareArgs = momentoLocalMiddlewareArgs;
  }

  @Override
  public MiddlewareRequestHandler onNewRequest(MiddlewareRequestHandlerContext context) {
    return new MomentoLocalMiddlewareRequestHandler(momentoLocalMiddlewareArgs);
  }
}
