package momento.sdk.config.transport;

import javax.annotation.Nullable;

/**
 * The simplest way to configure gRPC for the Momento client. Specifies static values the transport
 * options.
 */
public class StaticTransportStrategy implements TransportStrategy {

  private final GrpcConfiguration grpcConfiguration;
  private final Integer maxConcurrentRequests;

  /**
   * Constructs a StaticTransportStrategy.
   *
   * @param grpcConfiguration gRPC tunables.
   */
  public StaticTransportStrategy(GrpcConfiguration grpcConfiguration) {
    this.grpcConfiguration = grpcConfiguration;
    this.maxConcurrentRequests = null;
  }

  /**
   * Constructs a StaticTransportStrategy.
   *
   * @param grpcConfiguration gRPC tunables.
   * @param maxConcurrentRequests the maximum number of concurrent requests to Momento.
   */
  public StaticTransportStrategy(
      GrpcConfiguration grpcConfiguration, Integer maxConcurrentRequests) {
    this.grpcConfiguration = grpcConfiguration;
    this.maxConcurrentRequests = maxConcurrentRequests;
  }

  @Override
  public GrpcConfiguration getGrpcConfiguration() {
    return grpcConfiguration;
  }

  @Override
  public TransportStrategy withGrpcConfiguration(GrpcConfiguration grpcConfiguration) {
    return new StaticTransportStrategy(grpcConfiguration);
  }

  @Nullable
  @Override
  public Integer getMaxConcurrentRequests() {
    return maxConcurrentRequests;
  }

  @Override
  public TransportStrategy withMaxConcurrentRequests(int maxConcurrentRequests) {
    return new StaticTransportStrategy(grpcConfiguration, maxConcurrentRequests);
  }
}
