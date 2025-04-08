package momento.sdk.config.transport.topics;

import javax.annotation.Nullable;

/** Configuration for network tunables. */
public interface TopicsTransportStrategy {
  /**
   * Configures the low-level topics gRPC settings for the client's communication with the Momento
   * server.
   *
   * @return the topics gRPC configuration.
   */
  TopicsGrpcConfiguration getGrpcConfiguration();

  /**
   * Copy constructor that modifies the topics gRPC configuration.
   *
   * @param grpcConfiguration low-level topics gRPC settings.
   * @return The modified TopicsTransportStrategy.
   */
  TopicsTransportStrategy withGrpcConfiguration(TopicsGrpcConfiguration grpcConfiguration);

  /**
   * The maximum number of concurrent requests that the Momento client will allow onto the wire at a
   * given time.
   *
   * @return the max requests, or null if there is no maximum
   */
  @Nullable
  Integer getMaxConcurrentRequests();

  /**
   * Copy constructor that sets the maximum concurrent requests.
   *
   * @param maxConcurrentRequests the maximum number of concurrent requests to Momento.
   * @return The modified TransportStrategy.
   */
  TopicsTransportStrategy withMaxConcurrentRequests(int maxConcurrentRequests);
}
