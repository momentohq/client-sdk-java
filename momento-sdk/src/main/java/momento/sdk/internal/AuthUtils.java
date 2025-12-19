package momento.sdk.internal;

import java.util.Base64;
import momento.sdk.exceptions.InvalidArgumentException;

public class AuthUtils {

  public static boolean isV2ApiKey(String authToken) {
    try {
      // only v1 api keys are entirely b64 encoded
      // v2 keys are JWTs with b64 encoded segments
      if (isBase64Encoded(authToken)) {
        return false;
      }

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

  public static boolean isBase64Encoded(String apiKey) {
    try {
      Base64.getUrlDecoder().decode(apiKey);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static String getApiKeyValueFromEnvVar(String apiKeyEnvVar) {
    if (apiKeyEnvVar == null || apiKeyEnvVar.isEmpty()) { // Check for empty env var name
      throw new InvalidArgumentException("API key env var name cannot be empty");
    }

    String authToken = System.getenv(apiKeyEnvVar);
    if (authToken == null || authToken.isEmpty()) { // Check for empty value
      throw new InvalidArgumentException("Env var " + apiKeyEnvVar + " must be set");
    }
    return authToken;
  }

  public static String getEndpointValueFromEnvVar(String endpointEnvVar) {
    if (endpointEnvVar == null || endpointEnvVar.isEmpty()) { // Check for empty env var name
      throw new InvalidArgumentException("Endpoint env var name cannot be empty");
    }

    String authToken = System.getenv(endpointEnvVar);
    if (authToken == null || authToken.isEmpty()) { // Check for empty value
      throw new InvalidArgumentException(" Endpoint env var " + endpointEnvVar + " must be set");
    }
    return authToken;
  }
}
