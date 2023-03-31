package momento.sdk.config.transport;

/**
 * The simplest way to configure gRPC for the Momento client. Specifies static values the transport
 * options.
 */
public class StaticTransportStrategy implements TransportStrategy {

  private final GrpcConfiguration grpcConfiguration;

  public StaticTransportStrategy(GrpcConfiguration grpcConfiguration) {
    this.grpcConfiguration = grpcConfiguration;
  }

  @Override
  public GrpcConfiguration getGrpcConfiguration() {
    return grpcConfiguration;
  }

  @Override
  public TransportStrategy withGrpcConfiguration(GrpcConfiguration grpcConfiguration) {
    return new StaticTransportStrategy(grpcConfiguration);
  }
}
