package momento.sdk.auth;

import javax.annotation.Nonnull;

/**
 * Specifies the fields that are required for a Momento client to connect to and authenticate with
 * the Momento service.
 */
public abstract class CredentialProvider {

  /**
   * Creates a CredentialProvider using the provided auth token.
   *
   * @param authToken A Momento auth token.
   * @return The provider.
   */
  public static CredentialProvider fromString(@Nonnull String authToken) {
    return new StringCredentialProvider(authToken);
  }

  /**
   * Creates a CredentialProvider by loading an auth token from the provided environment variable.
   *
   * @param envVar An environment variable containing a Momento auth token.
   * @return The provider.
   */
  public static CredentialProvider fromEnvVar(@Nonnull String envVar) {
    return new EnvVarCredentialProvider(envVar);
  }

  /**
   * Gets the token used to authenticate to Momento.
   *
   * @return The token.
   */
  public abstract String getAuthToken();

  /**
   * Gets the endpoint with which the Momento client will connect to the Momento control plane.
   *
   * @return The endpoint.
   */
  public abstract String getControlEndpoint();

  /**
   * Gets the endpoint with which the Momento client will connect to the Momento data plane.
   *
   * @return The endpoint.
   */
  public abstract String getCacheEndpoint();
}
