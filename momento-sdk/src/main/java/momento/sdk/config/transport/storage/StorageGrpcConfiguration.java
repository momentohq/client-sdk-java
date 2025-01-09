package momento.sdk.config.transport.storage;

import static momento.sdk.ValidationUtils.ensureRequestDeadlineValid;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;
import momento.sdk.config.transport.IGrpcConfiguration;
import momento.sdk.internal.GrpcChannelOptions;

/** Abstracts away the gRPC configuration tunables. */
public class StorageGrpcConfiguration implements IGrpcConfiguration {

  private final Duration deadline;
  private final int minNumGrpcChannels;
  private final Optional<Integer> maxMessageSize;
  private final Optional<Boolean> keepAliveWithoutCalls;
  private final Optional<Integer> keepAliveTimeoutMs;
  private final Optional<Integer> keepAliveTimeMs;

  /**
   * Constructs a StorageGrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   */
  public StorageGrpcConfiguration(@Nonnull Duration deadline) {
    this(
        deadline,
        1,
        Optional.of(GrpcChannelOptions.DEFAULT_MAX_MESSAGE_SIZE),
        Optional.of(GrpcChannelOptions.DEFAULT_KEEPALIVE_WITHOUT_STREAM),
        Optional.of(GrpcChannelOptions.DEFAULT_KEEPALIVE_TIMEOUT_MS),
        Optional.of(GrpcChannelOptions.DEFAULT_KEEPALIVE_TIME_MS));
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
      Optional<Integer> maxMessageSize,
      Optional<Boolean> keepAliveWithoutCalls,
      Optional<Integer> keepAliveTimeout,
      Optional<Integer> keepAliveTime) {
    ensureRequestDeadlineValid(deadline);
    this.deadline = deadline;
    this.minNumGrpcChannels = minNumGrpcChannels;
    this.maxMessageSize = maxMessageSize;
    this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    this.keepAliveTimeoutMs = keepAliveTimeout;
    this.keepAliveTimeMs = keepAliveTime;
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
        keepAliveTimeoutMs,
        keepAliveTimeMs);
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
        keepAliveTimeoutMs,
        keepAliveTimeMs);
  }

  @Override
  public Optional<Integer> getMaxMessageSize() {
    return maxMessageSize;
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
        Optional.of(maxMessageSize),
        keepAliveWithoutCalls,
        keepAliveTimeoutMs,
        keepAliveTimeMs);
  }

  @Override
  public Optional<Boolean> getKeepAliveWithoutCalls() {
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
   * @param keepAliveWithoutCalls The boolean indicating whether to send keepalive pings without any
   *     active calls.
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withKeepAliveWithoutCalls(
      Optional<Boolean> keepAliveWithoutCalls) {
    return new StorageGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeoutMs,
        keepAliveTimeMs);
  }

  @Override
  public Optional<Integer> getKeepAliveTimeoutMs() {
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
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withKeepAliveTimeout(int keepAliveTimeoutMs) {
    return new StorageGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        Optional.of(keepAliveTimeoutMs),
        keepAliveTimeMs);
  }

  @Override
  public Optional<Integer> getKeepAliveTimeMs() {
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
   * @return The updated StorageGrpcConfiguration.
   */
  public StorageGrpcConfiguration withKeepAliveTime(int keepAliveTimeMs) {
    return new StorageGrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeoutMs,
        Optional.of(keepAliveTimeMs));
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
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }
}
