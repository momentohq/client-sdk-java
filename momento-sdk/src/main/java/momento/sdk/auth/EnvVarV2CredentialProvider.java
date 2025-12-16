package momento.sdk.auth;

import javax.annotation.Nonnull;
import momento.sdk.exceptions.InvalidArgumentException;

public class EnvVarV2CredentialProvider extends ApiKeyV2CredentialProvider {

  public EnvVarV2CredentialProvider(@Nonnull String apiKeyEnvVar, @Nonnull String endpointEnvVar) {
    super(getApiKeyValueFromEnvVar(apiKeyEnvVar), getEndpointValueFromEnvVar(endpointEnvVar));
  }

  private static String getApiKeyValueFromEnvVar(String apiKeyEnvVar) {
    if (apiKeyEnvVar == null || apiKeyEnvVar.isEmpty()) { // Check for empty env var name
      throw new InvalidArgumentException("Env var name cannot be empty");
    }

    String authToken = System.getenv(apiKeyEnvVar);
    if (authToken == null || authToken.isEmpty()) { // Check for empty value
      throw new InvalidArgumentException("Env var " + apiKeyEnvVar + " must be set");
    }
    return authToken;
  }

  private static String getEndpointValueFromEnvVar(String endpointEnvVar) {
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
