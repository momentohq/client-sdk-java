package momento.sdk.auth;

public class MomentoLocalProvider extends CredentialProvider {
  private final String cacheEndpoint;
  private final String controlEndpoint;
  private final String tokenEndpoint;
  private final String storageEndpoint;
  private final int port;

  private static final String DEFAULT_HOSTNAME = "127.0.0.1";
  private static final int DEFAULT_PORT = 8080;

  public MomentoLocalProvider(String hostname, int port) {
    this.cacheEndpoint = hostname;
    this.controlEndpoint = hostname;
    this.tokenEndpoint = hostname;
    this.storageEndpoint = hostname;
    this.port = port;
  }

  public MomentoLocalProvider(String hostname) {
    this(hostname, DEFAULT_PORT);
  }

  public MomentoLocalProvider(int port) {
    this(DEFAULT_HOSTNAME, port);
  }

  public MomentoLocalProvider() {
    this(DEFAULT_HOSTNAME, DEFAULT_PORT);
  }

  @Override
  public String getAuthToken() {
    return "";
  }

  @Override
  public String getCacheEndpoint() {
    return cacheEndpoint;
  }

  @Override
  public String getControlEndpoint() {
    return controlEndpoint;
  }

  @Override
  public String getTokenEndpoint() {
    return tokenEndpoint;
  }

  @Override
  public String getStorageEndpoint() {
    return storageEndpoint;
  }

  @Override
  public boolean isEndpointSecure(String endpoint) {
    return isSecureConnection(endpoint);
  }

  @Override
  public int getPort() {
    return port;
  }

  private boolean isSecureConnection(String endpoint) {
    return endpoint.startsWith("https://");
  }
}
