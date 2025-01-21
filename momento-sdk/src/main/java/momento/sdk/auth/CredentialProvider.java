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
   * Creates a CredentialProvider using the provided MomentoLocalProviderProps.
   * @return The Momento local provider.
   */
  public static CredentialProvider forMomentoLocal() {
    String defaultHostname = "127.0.0.1";
    int defaultPort = 8080;
    return new MomentoLocalProvider(defaultHostname, defaultPort);
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
   * Gets whether the control plane endpoint connection is secure.
   *
   * @return true if connecting to the control plane endpoint connection with TLS; false if not using TLS
   */
  public abstract boolean isControlEndpointSecure();

  /**
   * Gets the endpoint with which the Momento client will connect to the Momento data plane.
   *
   * @return The endpoint.
   */
  public abstract String getCacheEndpoint();


  /**
   * Gets whether the data plane endpoint connection is secure.
   *
   * @return true if connecting to the data plane endpoint connection with TLS; false if not using TLS
   */
  public abstract boolean isCacheEndpointSecure();

  /**
   * Gets the endpoint with which the Momento client will connect to the Momento storage service.
   *
   * @return The endpoint.
   */
  public abstract String getStorageEndpoint();

  /**
   * Gets whether the storage endpoint connection is secure.
   *
   * @return true if connecting to the storage endpoint connection with TLS; false if not using TLS
   */
  public abstract boolean isStorageEndpointSecure();

  /**
   * Gets the token endpoint with which the Momento client will connect to the Momento token
   * service.
   *
   * @return The token endpoint.
   */
  public abstract String getTokenEndpoint();

  /**
   * Gets whether the token endpoint connection is secure.
   *
   * @return true if connecting to the token endpoint connection with TLS; false if not using TLS
   */
  public abstract boolean isTokenEndpointSecure();
}
