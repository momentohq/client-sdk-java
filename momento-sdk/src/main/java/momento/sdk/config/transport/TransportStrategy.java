package momento.sdk.config.transport;

/** Configuration for network tunables. */
public interface TransportStrategy {
  /**
   * Configures the low-level gRPC settings for the client's communication with the Momento server.
   *
   * @return the gRPC configuration.
   */
  GrpcConfiguration getGrpcConfiguration();

  TransportStrategy withGrpcConfiguration(GrpcConfiguration grpcConfiguration);
}
