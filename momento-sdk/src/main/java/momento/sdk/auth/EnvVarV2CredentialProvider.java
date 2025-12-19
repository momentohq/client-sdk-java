package momento.sdk.auth;

import javax.annotation.Nonnull;
import momento.sdk.internal.AuthUtils;

public class EnvVarV2CredentialProvider extends ApiKeyV2CredentialProvider {

  public EnvVarV2CredentialProvider(@Nonnull String apiKeyEnvVar, @Nonnull String endpointEnvVar) {
    super(
        AuthUtils.getApiKeyValueFromEnvVar(apiKeyEnvVar),
        AuthUtils.getApiKeyValueFromEnvVar(endpointEnvVar));
  }
}
