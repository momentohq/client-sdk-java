package momento.sdk.config.transport.topics;

import javax.annotation.Nullable;

/**
 * The simplest way to configure gRPC for the Momento client. Specifies static values the transport
 * options.
 */
public class StaticTopicsTransportStrategy implements TopicsTransportStrategy {

  private final TopicsGrpcConfiguration grpcConfiguration;
  private final Integer maxConcurrentRequests;

  /**
   * Constructs a StaticTransportStrategy.
   *
   * @param grpcConfiguration gRPC tunables.
   */
  public StaticTopicsTransportStrategy(TopicsGrpcConfiguration grpcConfiguration) {
    this.grpcConfiguration = grpcConfiguration;
    this.maxConcurrentRequests = null;
  }

  /**
   * Constructs a StaticTransportStrategy.
   *
   * @param grpcConfiguration gRPC tunables.
   * @param maxConcurrentRequests the maximum number of concurrent requests to Momento.
   */
  public StaticTopicsTransportStrategy(
      TopicsGrpcConfiguration grpcConfiguration, Integer maxConcurrentRequests) {
    this.grpcConfiguration = grpcConfiguration;
    this.maxConcurrentRequests = maxConcurrentRequests;
  }

  @Override
  public TopicsGrpcConfiguration getGrpcConfiguration() {
    return grpcConfiguration;
  }

  @Override
  public TopicsTransportStrategy withGrpcConfiguration(TopicsGrpcConfiguration grpcConfiguration) {
    return new StaticTopicsTransportStrategy(grpcConfiguration);
  }

  @Nullable
  @Override
  public Integer getMaxConcurrentRequests() {
    return maxConcurrentRequests;
  }

  @Override
  public TopicsTransportStrategy withMaxConcurrentRequests(int maxConcurrentRequests) {
    return new StaticTopicsTransportStrategy(grpcConfiguration, maxConcurrentRequests);
  }
}
