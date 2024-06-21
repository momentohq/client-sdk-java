package momento.sdk.config.transport.storage;

/** Configuration for network tunables. */
public interface StorageTransportStrategy {
  /**
   * Configures the low-level gRPC settings for the client's communication with the Momento server.
   *
   * @return the gRPC configuration.
   */
  StorageGrpcConfiguration getGrpcConfiguration();

  /**
   * Copy constructor that modifies the gRPC configuration.
   *
   * @param grpcConfiguration low-level gRPC settings.
   * @return The modified StorageTransportStrategy.
   */
  StorageTransportStrategy withGrpcConfiguration(StorageGrpcConfiguration grpcConfiguration);
}
