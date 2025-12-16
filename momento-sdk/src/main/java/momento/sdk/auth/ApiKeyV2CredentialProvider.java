package momento.sdk.auth;

import static momento.sdk.internal.AuthUtils.isV2ApiKey;

import javax.annotation.Nonnull;
import momento.sdk.exceptions.InvalidArgumentException;

public class ApiKeyV2CredentialProvider extends CredentialProvider {

  private final String authToken;
  private final String controlEndpoint;
  private final String cacheEndpoint;
  private final String storageEndpoint;
  private final String tokenEndpoint;

  private static String build(String prefix, String endpoint) {
    return prefix + "." + endpoint;
  }

  public ApiKeyV2CredentialProvider(@Nonnull String authToken, @Nonnull String endpoint) {
    if (authToken.isEmpty()) {
      throw new InvalidArgumentException("API key must not be empty");
    }
    if (endpoint.isEmpty()) {
      throw new InvalidArgumentException("Endpoint must not be empty");
    }

    if (!isV2ApiKey(authToken)) {
      throw new InvalidArgumentException(
          "Received an invalid v2 API key. Did you mean to use `fromString()` or `fromEnvVar()` with a legacy key instead?");
    }

    this.authToken = authToken;
    this.controlEndpoint = build("control", endpoint);
    this.cacheEndpoint = build("cache", endpoint);
    this.storageEndpoint = build("storage", endpoint);
    this.tokenEndpoint = build("token", endpoint);
  }

  @Override
  public String getAuthToken() {
    return authToken;
  }

  @Override
  public String getControlEndpoint() {
    return controlEndpoint;
  }

  @Override
  public String getCacheEndpoint() {
    return cacheEndpoint;
  }

  @Override
  public String getStorageEndpoint() {
    return storageEndpoint;
  }

  @Override
  public String getTokenEndpoint() {
    return tokenEndpoint;
  }

  @Override
  public boolean isEndpointSecure() {
    return true;
  }

  @Override
  public int getPort() {
    return 443;
  }
}
