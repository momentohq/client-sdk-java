package momento.sdk.retry.utils;

import momento.sdk.config.middleware.Middleware;
import momento.sdk.config.middleware.MiddlewareRequestHandler;
import momento.sdk.config.middleware.MiddlewareRequestHandlerContext;

public class TestRetryMetricsMiddleware implements Middleware {
  private final TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs;

  public TestRetryMetricsMiddleware(TestRetryMetricsMiddlewareArgs testRetryMetricsMiddlewareArgs) {
    this.testRetryMetricsMiddlewareArgs = testRetryMetricsMiddlewareArgs;
  }

  @Override
  public MiddlewareRequestHandler onNewRequest(MiddlewareRequestHandlerContext context) {
    return new TestRetryMetricsMiddlewareRequestHandler(testRetryMetricsMiddlewareArgs);
  }
}
