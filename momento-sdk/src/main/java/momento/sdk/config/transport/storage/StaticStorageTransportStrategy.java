package momento.sdk.config.transport.storage;

/**
 * The simplest way to configure gRPC for the Momento client. Specifies static values the transport
 * options.
 */
public class StaticStorageTransportStrategy implements StorageTransportStrategy {

  private final StorageGrpcConfiguration grpcConfiguration;

  /**
   * Constructs a StaticStorageTransportStrategy.
   *
   * @param grpcConfiguration gRPC tunables.
   */
  public StaticStorageTransportStrategy(StorageGrpcConfiguration grpcConfiguration) {
    this.grpcConfiguration = grpcConfiguration;
  }

  @Override
  public StorageGrpcConfiguration getGrpcConfiguration() {
    return grpcConfiguration;
  }

  @Override
  public StorageTransportStrategy withGrpcConfiguration(
      StorageGrpcConfiguration grpcConfiguration) {
    return new StaticStorageTransportStrategy(grpcConfiguration);
  }
}
