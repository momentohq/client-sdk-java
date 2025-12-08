package momento.sdk.auth;

import java.util.Base64;
import javax.annotation.Nonnull;
import momento.sdk.exceptions.InvalidArgumentException;

public class GlobalStringCredentialProvider extends CredentialProvider {
  private final String authToken;
  private final String controlEndpoint;
  private final String cacheEndpoint;
  private final String storageEndpoint;
  private final String tokenEndpoint;

  private static String build(String prefix, String endpoint) {
    return prefix + "." + endpoint;
  }

  private static boolean isGlobalApiKey(String authToken) {
    try {
      // JWT tokens have 3 parts separated by dots
      if (authToken.chars().filter(ch -> ch == '.').count() != 2) {
        return false;
      }

      // Split and get the payload (second part)
      String[] parts = authToken.split("\\.");
      if (parts.length != 3) {
        return false;
      }

      // Decode the payload from base64
      String payload = new String(Base64.getUrlDecoder().decode(parts[1]));

      // Check if it contains "t":"g" (global key indicator)
      return payload.contains("\"t\"") && payload.contains("\"g\"");
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean isBase64EncodedToken(String apiKey) {
    // Check if it's a global JWT (which is allowed)
    if (isGlobalApiKey(apiKey)) {
      return false;
    }

    // Legacy tokens have format: xxx.yyy.zzz (JWT format)
    if (apiKey.chars().filter(ch -> ch == '.').count() == 2) {
      return true;
    }

    // Check if it's base64 encoded (V1 tokens are base64 encoded)
    try {
      Base64.getUrlDecoder().decode(apiKey);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public GlobalStringCredentialProvider(@Nonnull String authToken, @Nonnull String endpoint) {
    if (authToken == null || authToken == "") {
      throw new InvalidArgumentException("Auth token must not be empty");
    }
    if (endpoint == null || endpoint == "") {
      throw new InvalidArgumentException("Endpoint must not be empty");
    }

    if (isBase64EncodedToken(authToken)) {
      throw new InvalidArgumentException(
          "Global API key appears to be a V1 or legacy token. "
              + "Please use CredentialProvider.fromString() instead of globalKeyFromString()");
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
