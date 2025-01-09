package momento.sdk.config.transport.storage;

import static momento.sdk.ValidationUtils.ensureRequestDeadlineValid;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.config.transport.IGrpcConfiguration;
import momento.sdk.internal.GrpcChannelOptions;

/** Abstracts away the gRPC configuration tunables. */
public class StorageGrpcConfiguration implements IGrpcConfiguration {

  private final @Nonnull Duration deadline;
  private final int minNumGrpcChannels;
  private final @Nullable Integer maxMessageSize;
  private final @Nullable Boolean keepAliveWithoutCalls;
  private final @Nullable Duration keepAliveTimeout;
  private final @Nullable Duration keepAliveTime;

  /**
   * Constructs a StorageGrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   */
  public StorageGrpcConfiguration(@Nonnull Duration deadline) {
    this(
        deadline,
        1,
        GrpcChannelOptions.DEFAULT_MAX_MESSAGE_SIZE,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_WITHOUT_STREAM,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_TIMEOUT,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_TIME);
  }

  /**
   * Constructs a StorageGrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   * @param minNumGrpcChannels The minimum number of gRPC channels to keep open at any given time.
   * @param maxMessageSize The maximum size of a message (in bytes) that can be received by the
   *     client.
   * @param keepAliveWithoutCalls Whether to send keepalive pings without any active calls.
   * @param keepAliveTimeout The time in milliseconds to wait for a keepalive ping response before
   *     considering the connection dead.
   * @param keepAliveTime The time in milliseconds to wait between keepalive pings.
   */
  public StorageGrpcConfiguration(
      @Nonnull Duration deadline,
      int minNumGrpcChannels,
      Optional<Integer> maxMessageSize,
      Optional<Boolean> keepAliveWithoutCalls,
      Optional<Integer> keepAliveTimeout,
      Optional<Integer> keepAliveTime) {
    this(
        deadline,
        minNumGrpcChannels,
        maxMessageSize.orElse(null),
        keepAliveWithoutCalls.orElse(null),
        keepAliveTimeout.map(Duration::ofMillis).orElse(null),
        keepAliveTime.map(Duration::ofMillis).orElse(null));
  }

  /**
   * Constructs a StorageGrpcConfiguration.
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
  public StorageGrpcConfiguration(
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

  @Override
  public Duration getDeadline() {
    return deadline;
  }

  /**
   * Copy constructor that updates the deadline.
   *
   * @param deadline The new deadline.
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withDeadline(Duration deadline) {
    return new StorageGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  @Override
  public int getMinNumGrpcChannels() {
    return minNumGrpcChannels;
  }

  /**
   * Copy constructor that updates the minimum number of gRPC channels.
   *
   * @param minNumGrpcChannels The new minimum number of gRPC channels.
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withMinNumGrpcChannels(int minNumGrpcChannels) {
    return new StorageGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  /**
   * The maximum size of a message (in bytes) that can be received by the client.
   *
   * @return the maximum message size, or empty if there is no specified maximum.
   */
  public Optional<Integer> getMaxMessageSize() {
    return getMaxReceivedMessageSize();
  }

  @Override
  public Optional<Integer> getMaxReceivedMessageSize() {
    return Optional.ofNullable(maxMessageSize);
  }

  /**
   * Copy constructor that updates the maximum message size.
   *
   * @param maxMessageSize The new maximum message size.
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withMaxMessageSize(int maxMessageSize) {
    return new StorageGrpcConfiguration(
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
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withKeepAliveWithoutCalls(
      Optional<Boolean> keepAliveWithoutCalls) {
    return withKeepAliveWithoutCalls(keepAliveWithoutCalls.orElse(null));
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
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withKeepAliveWithoutCalls(
      @Nullable Boolean keepAliveWithoutCalls) {
    return new StorageGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  /**
   * The time to wait for a keepalive ping response before considering the connection dead.
   *
   * @return the time to wait for a keepalive ping response before considering the connection dead.
   */
  public Optional<Integer> getKeepAliveTimeoutMs() {
    return getKeepAliveTimeout().map(d -> (int) d.toMillis());
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
   * @param keepAliveTimeoutMs The new time to wait for a keepalive ping response.
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withKeepAliveTimeout(int keepAliveTimeoutMs) {
    return new StorageGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        Duration.ofMillis(keepAliveTimeoutMs),
        keepAliveTime);
  }

  /**
   * The time to wait between keepalive pings.
   *
   * @return the time to wait between keepalive pings.
   */
  public Optional<Integer> getKeepAliveTimeMs() {
    return getKeepAliveTime().map(d -> (int) d.toMillis());
  }

  @Override
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
   * @param keepAliveTimeMs The new time to wait between keepalive pings.
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withKeepAliveTime(int keepAliveTimeMs) {
    return new StorageGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        Duration.ofMillis(keepAliveTimeMs));
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
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withKeepAliveDisabled() {
    return new StorageGrpcConfiguration(
        deadline, minNumGrpcChannels, maxMessageSize, null, null, null);
  }
}
