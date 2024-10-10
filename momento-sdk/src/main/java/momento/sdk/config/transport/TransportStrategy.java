package momento.sdk.config.transport;

import javax.annotation.Nullable;

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
  TransportStrategy withMaxConcurrentRequests(int maxConcurrentRequests);
}
