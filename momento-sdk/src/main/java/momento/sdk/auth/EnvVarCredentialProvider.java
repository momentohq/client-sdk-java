package momento.sdk.auth;

import javax.annotation.Nonnull;

/**
 * Parses connection and authentication information from a JWT read from an environment variable.
 */
public class EnvVarCredentialProvider extends StringCredentialProvider {

  /**
   * Parses connection and authentication information from a JWT read from the given environment
   * variable
   *
   * @param envVarName the environment variable containing the Momento JWT
   */
  public EnvVarCredentialProvider(@Nonnull String envVarName) {
    super(System.getenv(envVarName));
  }
}
