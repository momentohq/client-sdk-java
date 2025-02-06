package momento.sdk.config.middleware;

import java.util.Map;

public interface MiddlewareRequestHandlerContext {
  Map<String, String> getContext();
}
