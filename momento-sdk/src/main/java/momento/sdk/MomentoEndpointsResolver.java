package momento.sdk;

import java.util.Optional;
import momento.sdk.exceptions.ClientSdkException;

final class MomentoEndpointsResolver {

  private static final String CONTROL_ENDPOINT_PREFIX = "control.";
  private static final String CACHE_ENDPOINT_PREFIX = "cache.";

  static MomentoEndpoints resolve(String authToken, Optional<String> hostedZone) {
    AuthTokenParser.Claims claims = AuthTokenParser.parse(authToken);
    String controlEndpoint = getControlEndpoint(claims, hostedZone);
    String cacheEndpoint = getCacheEndpoint(claims, hostedZone);
    return new MomentoEndpoints(controlEndpoint, cacheEndpoint);
  }

  private static String getControlEndpoint(
      AuthTokenParser.Claims claims, Optional<String> hostedZone) {
    return controlEndpointFromHostedZone(hostedZone)
        .orElseGet(
            () ->
                claims
                    .controlEndpoint()
                    .orElseThrow(
                        () ->
                            new ClientSdkException(
                                "Failed to determine control endpoint from the auth token or an override")));
  }

  private static String getCacheEndpoint(
      AuthTokenParser.Claims claims, Optional<String> hostedZone) {
    return cacheEndpointFromHostedZone(hostedZone)
        .orElseGet(
            () ->
                claims
                    .cacheEndpoint()
                    .orElseThrow(
                        () ->
                            new ClientSdkException(
                                "Failed to determine cache endpoint from the auth token or an override")));
  }

  private static Optional<String> controlEndpointFromHostedZone(Optional<String> hostedZone) {
    if (hostedZone.isPresent()) {
      return Optional.of(CONTROL_ENDPOINT_PREFIX + hostedZone.get());
    }
    return Optional.empty();
  }

  private static Optional<String> cacheEndpointFromHostedZone(Optional<String> hostedZone) {
    if (hostedZone.isPresent()) {
      return Optional.of(CACHE_ENDPOINT_PREFIX + hostedZone.get());
    }
    return Optional.empty();
  }

  static class MomentoEndpoints {
    private final String controlEndpoint;
    private final String cacheEndpoint;

    private MomentoEndpoints(String controlEndpoint, String cacheEndpoint) {
      this.cacheEndpoint = cacheEndpoint;
      this.controlEndpoint = controlEndpoint;
    }

    String controlEndpoint() {
      return this.controlEndpoint;
    }

    String cacheEndpoint() {
      return this.cacheEndpoint;
    }
  }
}
