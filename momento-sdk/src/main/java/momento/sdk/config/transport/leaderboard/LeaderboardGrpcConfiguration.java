package momento.sdk.config.transport.leaderboard;

import static momento.sdk.ValidationUtils.ensureRequestDeadlineValid;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.config.transport.IGrpcConfiguration;
import momento.sdk.internal.GrpcChannelOptions;

/** Abstracts away the gRPC configuration tunables. */
public class LeaderboardGrpcConfiguration implements IGrpcConfiguration {

  private final @Nonnull Duration deadline;
  private final int minNumGrpcChannels;
  private final @Nullable Integer maxMessageSize;
  private final @Nullable Boolean keepAliveWithoutCalls;
  private final @Nullable Duration keepAliveTimeout;
  private final @Nullable Duration keepAliveTime;

  /**
   * Constructs a LeaderboardGrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   */
  public LeaderboardGrpcConfiguration(@Nonnull Duration deadline) {
    this(
        deadline,
        1,
        GrpcChannelOptions.DEFAULT_LEADERBOARD_MAX_MESSAGE_SIZE,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_WITHOUT_STREAM,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_TIMEOUT,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_TIME);
  }

  /**
   * Constructs a LeaderboardGrpcConfiguration.
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
  public LeaderboardGrpcConfiguration(
      @Nonnull Duration deadline,
      int minNumGrpcChannels,
      @Nullable Integer maxMessageSize,
      @Nullable Boolean keepAliveWithoutCalls,
      @Nullable Duration keepAliveTimeout,
      @Nullable Duration keepAliveTime) {
    ensureRequestDeadlineValid(deadline);
    this.deadline = deadline;
    this.minNumGrpcChannels = minNumGrpcChannels;
    this.maxMessageSize = maxMessageSize;
    this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    this.keepAliveTimeout = keepAliveTimeout;
    this.keepAliveTime = keepAliveTime;
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

  @Override
  public LeaderboardGrpcConfiguration withDeadline(Duration deadline) {
    return new LeaderboardGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
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
   * @return The updated LeaderboardGrpcConfiguration.
   */
  public LeaderboardGrpcConfiguration withMinNumGrpcChannels(int minNumGrpcChannels) {
    return new LeaderboardGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  @Override
  public Optional<Integer> getMaxReceivedMessageSize() {
    return Optional.ofNullable(maxMessageSize);
  }

  /**
   * Copy constructor that updates the maximum message size.
   *
   * @param maxMessageSize The new maximum message size.
   * @return The updated LeaderboardGrpcConfiguration.
   */
  public LeaderboardGrpcConfiguration withMaxMessageSize(int maxMessageSize) {
    return new LeaderboardGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  @Override
  public Optional<Boolean> getKeepAliveWithoutCalls() {
    return Optional.ofNullable(keepAliveWithoutCalls);
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
   * @param keepAliveWithoutCalls The boolean indicating whether to send keepalive pings without any
   *     active calls.
   * @return The updated LeaderboardGrpcConfiguration.
   */
  public LeaderboardGrpcConfiguration withKeepAliveWithoutCalls(boolean keepAliveWithoutCalls) {
    return new LeaderboardGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  @Override
  public Optional<Duration> getKeepAliveTimeout() {
    return Optional.ofNullable(keepAliveTimeout);
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
   * @param keepAliveTimeout The new time to wait for a keepalive ping response.
   * @return The updated LeaderboardGrpcConfiguration.
   */
  public LeaderboardGrpcConfiguration withKeepAliveTimeout(Duration keepAliveTimeout) {
    return new LeaderboardGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  /**
   * The time to wait between keepalive pings.
   *
   * @return the time to wait between keepalive pings.
   */
  public Optional<Duration> getKeepAliveTime() {
    return Optional.ofNullable(keepAliveTime);
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
   * @param keepAliveTime The new time to wait between keepalive pings.
   * @return The updated LeaderboardGrpcConfiguration.
   */
  public LeaderboardGrpcConfiguration withKeepAliveTime(Duration keepAliveTime) {
    return new LeaderboardGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
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
   * @return The updated LeaderboardGrpcConfiguration.
   */
  public LeaderboardGrpcConfiguration withKeepAliveDisabled() {
    return new LeaderboardGrpcConfiguration(
        deadline, minNumGrpcChannels, maxMessageSize, null, null, null);
  }
}
