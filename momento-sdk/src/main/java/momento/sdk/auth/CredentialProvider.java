package momento.sdk.auth;

/**
 * Specifies the fields that are required for a Momento client to connect to and authenticate with
 * the Momento service.
 */
public interface CredentialProvider {

  /**
   * The JWT used to authenticate to Momento.
   *
   * @return the JWT
   */
  String getAuthToken();

  /**
   * The endpoint with which the Momento client will connect to the Momento control plane.
   *
   * @return the endpoint
   */
  String getControlEndpoint();

  /**
   * The endpoint with which the Momento client will connect to the Momento data plane.
   *
   * @return the endpoint
   */
  String getCacheEndpoint();
}
