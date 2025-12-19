package momento.sdk.auth;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.internal.AuthUtils;

/**
 * Parses connection and authentication information from a JWT read from an environment variable.
 */
public class EnvVarCredentialProvider extends StringCredentialProvider {

  /**
   * Parses connection and authentication information from an authentication token read from the
   * given environment variable.
   *
   * @param envVarName the environment variable containing the Momento authentication token.
   */
  public EnvVarCredentialProvider(@Nonnull String envVarName) {
    super(AuthUtils.getApiKeyValueFromEnvVar(envVarName), null, null, null, null);
  }

  /**
   * Parses connection and authentication information from an authentication token read from the
   * given environment variable.
   *
   * @param envVarName the environment variable containing the Momento authentication token.
   * @param controlHost URI to use for control plane operations.
   * @param cacheHost URI to use for data plane operations.
   * @param storageHost URI to use for storage operations.
   */
  public EnvVarCredentialProvider(
      @Nonnull String envVarName,
      @Nullable String controlHost,
      @Nullable String cacheHost,
      @Nullable String storageHost) {
    super(
        AuthUtils.getApiKeyValueFromEnvVar(envVarName), controlHost, cacheHost, storageHost, null);
  }
}
