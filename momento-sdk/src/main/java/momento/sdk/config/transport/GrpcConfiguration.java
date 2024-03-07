package momento.sdk.config.transport;

import static momento.sdk.ValidationUtils.ensureRequestDeadlineValid;

import java.time.Duration;
import java.util.OptionalInt;
import javax.annotation.Nonnull;
import momento.sdk.internal.GrpcChannelOptions;

/** Abstracts away the gRPC configuration tunables. */
public class GrpcConfiguration {

  private final Duration deadline;
  private final int minNumGrpcChannels;
  private final OptionalInt maxMessageSize;
  private final boolean keepAliveWithoutCalls;
  private final OptionalInt keepAliveTimeoutMs;
  private final OptionalInt keepAliveTimeMs;

  /**
   * Constructs a GrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   */
  public GrpcConfiguration(@Nonnull Duration deadline) {
    this(
        deadline,
        1,
        OptionalInt.of(GrpcChannelOptions.DEFAULT_MAX_MESSAGE_SIZE),
        GrpcChannelOptions.DEFAULT_KEEPALIVE_WITHOUT_STREAM,
        OptionalInt.of(GrpcChannelOptions.DEFAULT_KEEPALIVE_TIMEOUT_MS),
        OptionalInt.of(GrpcChannelOptions.DEFAULT_KEEPALIVE_TIME_MS));
  }

  /**
   * Constructs a GrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   * @param minNumGrpcChannels The minimum number of gRPC channels to keep open at any given time.
   * @param maxMessageSize The maximum size of a message (in bytes) that can be received by the
   *     client.
   * @param keepAliveWithoutCalls Whether to send keepalive pings without any active calls.
   * @param keepAliveTimeout The time to wait for a keepalive ping response before considering the
   *     connection dead.
   * @param keepAliveTime The time to wait between keepalive pings.
   */
  public GrpcConfiguration(
      @Nonnull Duration deadline,
      int minNumGrpcChannels,
      OptionalInt maxMessageSize,
      boolean keepAliveWithoutCalls,
      OptionalInt keepAliveTimeout,
      OptionalInt keepAliveTime) {
    ensureRequestDeadlineValid(deadline);
    this.deadline = deadline;
    this.minNumGrpcChannels = minNumGrpcChannels;
    this.maxMessageSize = maxMessageSize;
    this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    this.keepAliveTimeoutMs = keepAliveTimeout;
    this.keepAliveTimeMs = keepAliveTime;
  }

  /**
   * How long the client will wait for an RPC to complete before it is terminated with {@link
   * io.grpc.Status.Code#DEADLINE_EXCEEDED}.
   *
   * @return the deadline
   */
  public Duration getDeadline() {
    return deadline;
  }

  /**
   * Copy constructor that updates the deadline.
   *
   * @param deadline The new deadline.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withDeadline(Duration deadline) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeoutMs,
        keepAliveTimeMs);
  }

  /**
   * The minimum number of gRPC channels to keep open at any given time.
   *
   * @return the minimum number of gRPC channels.
   */
  public int getMinNumGrpcChannels() {
    return minNumGrpcChannels;
  }

  /**
   * Copy constructor that updates the minimum number of gRPC channels.
   *
   * @param minNumGrpcChannels The new minimum number of gRPC channels.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withMinNumGrpcChannels(int minNumGrpcChannels) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeoutMs,
        keepAliveTimeMs);
  }

  /**
   * The maximum size of a message (in bytes) that can be received by the client.
   *
   * @return the maximum message size.
   */
  public OptionalInt getMaxMessageSize() {
    return maxMessageSize;
  }

  /**
   * Copy constructor that updates the maximum message size.
   *
   * @param maxMessageSize The new maximum message size.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withMaxMessageSize(int maxMessageSize) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        OptionalInt.of(maxMessageSize),
        keepAliveWithoutCalls,
        keepAliveTimeoutMs,
        keepAliveTimeMs);
  }

  /**
   * Whether keepalive will be performed when there are no outstanding requests on a connection.
   *
   * @return the boolean indicating whether to send keepalive pings without any active calls.
   */
  public boolean getKeepAliveWithoutCalls() {
    return keepAliveWithoutCalls;
  }

  /**
   * Copy constructor that updates whether keepalive will be performed when there are no outstanding
   * requests on a connection.
   *
   * <p>NOTE: keep-alives are very important for long-lived server environments where there may be
   * periods of time when the connection is idle. However, they are very problematic for lambda
   * environments where the lambda runtime is continuously frozen and unfrozen, because the lambda
   * may be frozen before the "ACK" is received from the server. This can cause the keep-alive to
   * timeout even though the connection is completely healthy. Therefore, keep-alives should be
   * disabled in lambda and similar environments.
   *
   * @param keepAliveWithoutCalls The new boolean indicating whether to send keepalive pings without
   *     any active calls.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withKeepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeoutMs,
        keepAliveTimeMs);
  }

  /**
   * The time to wait for a keepalive ping response before considering the connection dead.
   *
   * @return the time to wait for a keepalive ping response before considering the connection dead.
   */
  public OptionalInt getKeepAliveTimeoutMs() {
    return keepAliveTimeoutMs;
  }

  /**
   * Copy constructor that updates the time to wait for a keepalive ping response before considering
   * the connection dead.
   *
   * <p>NOTE: keep-alives are very important for long-lived server environments where there may be
   * periods of time when the connection is idle. However, they are very problematic for lambda
   * environments where the lambda runtime is continuously frozen and unfrozen, because the lambda
   * may be frozen before the "ACK" is received from the server. This can cause the keep-alive to
   * timeout even though the connection is completely healthy. Therefore, keep-alives should be
   * disabled in lambda and similar environments.
   *
   * @param keepAliveTimeoutMs The new time to wait for a keepalive ping response.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withKeepAliveTimeout(int keepAliveTimeoutMs) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        OptionalInt.of(keepAliveTimeoutMs),
        keepAliveTimeMs);
  }

  /**
   * The time to wait between keepalive pings.
   *
   * @return the time to wait between keepalive pings.
   */
  public OptionalInt getKeepAliveTimeMs() {
    return keepAliveTimeMs;
  }

  /**
   * Copy constructor that updates the time to wait between keepalive pings.
   *
   * <p>NOTE: keep-alives are very important for long-lived server environments where there may be
   * periods of time when the connection is idle. However, they are very problematic for lambda
   * environments where the lambda runtime is continuously frozen and unfrozen, because the lambda
   * may be frozen before the "ACK" is received from the server. This can cause the keep-alive to
   * timeout even though the connection is completely healthy. Therefore, keep-alives should be
   * disabled in lambda and similar environments.
   *
   * @param keepAliveTimeMs The new time to wait between keepalive pings.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withKeepAliveTime(int keepAliveTimeMs) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeoutMs,
        OptionalInt.of(keepAliveTimeMs));
  }

  /**
   * Copy constructor that disables all client-side keepalive settings.
   *
   * <p>NOTE: keep-alives are very important for long-lived server environments where there may be
   * periods of time when the connection is idle. However, they are very problematic for lambda
   * environments where the lambda runtime is continuously frozen and unfrozen, because the lambda
   * may be frozen before the "ACK" is received from the server. This can cause the keep-alive to
   * timeout even though the connection is completely healthy. Therefore, keep-alives should be
   * disabled in lambda and similar environments.
   *
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withKeepAliveDisabled() {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        false,
        OptionalInt.empty(),
        OptionalInt.empty());
  }
}
