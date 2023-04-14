package momento.sdk.auth;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Parses connection and authentication information from a JWT read from an environment variable.
 */
public class EnvVarCredentialProvider extends StringCredentialProvider {

  /**
   * Parses connection and authentication information from an authentication token read from
   * the given environment variable.
   *
   * @param envVarName the environment variable containing the Momento authentication token.
   */
  public EnvVarCredentialProvider(@Nonnull String envVarName) {
    super(System.getenv(envVarName), null, null);
  }

  /**
   * Parses connection and authentication information from an authentication token read from
   * the given environment variable.
   *
   * @param envVarName the environment variable containing the Momento authentication token.
   * @param controlHost URI to use for control plane operations.
   * @param cacheHost URI to use for data plane operations.
   */
  public EnvVarCredentialProvider(
          @Nonnull String envVarName, @Nullable String controlHost, @Nullable String cacheHost
  ) {
    super(System.getenv(envVarName), controlHost, cacheHost);
  }
}
