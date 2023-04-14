package momento.sdk.config.transport;

/** Configuration for network tunables. */
public interface TransportStrategy {
  /**
   * Configures the low-level gRPC settings for the client's communication with the Momento server.
   *
   * @return the gRPC configuration.
   */
  GrpcConfiguration getGrpcConfiguration();

  /**
   * Copy constructor that modifies the gRPC configuration.
   *
   * @param grpcConfiguration low-level gRPC settings.
   * @return The modified TransportStrategy.
   */
  TransportStrategy withGrpcConfiguration(GrpcConfiguration grpcConfiguration);
}
