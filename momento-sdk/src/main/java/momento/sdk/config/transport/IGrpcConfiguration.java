package momento.sdk.config.transport;

import java.time.Duration;
import java.util.Optional;

public interface IGrpcConfiguration {
  /**
   * How long the client will wait for an RPC to complete before it is terminated with {@link
   * io.grpc.Status.Code#DEADLINE_EXCEEDED}.
   *
   * @return the deadline
   */
  Duration getDeadline();

  /**
   * The minimum number of gRPC channels to keep open at any given time.
   *
   * @return the minimum number of gRPC channels.
   */
  int getMinNumGrpcChannels();

  /**
   * The maximum size of a message (in bytes) that can be received by the client.
   *
   * @return the maximum message size, or empty if there is no specified maximum.
   */
  Optional<Integer> getMaxMessageSize();

  /**
   * Whether keepalive will be performed when there are no outstanding requests on a connection.
   *
   * @return the boolean indicating whether to send keepalive pings without any active calls.
   */
  Optional<Boolean> getKeepAliveWithoutCalls();

  /**
   * The time to wait for a keepalive ping response before considering the connection dead.
   *
   * @return the time to wait for a keepalive ping response before considering the connection dead.
   */
  Optional<Integer> getKeepAliveTimeoutMs();

  /**
   * The time to wait between keepalive pings.
   *
   * @return the time to wait between keepalive pings.
   */
  Optional<Integer> getKeepAliveTimeMs();
}
