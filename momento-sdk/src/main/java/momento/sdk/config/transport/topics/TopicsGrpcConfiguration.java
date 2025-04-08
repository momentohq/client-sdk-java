package momento.sdk.config.transport.topics;

import static momento.sdk.ValidationUtils.ensureRequestDeadlineValid;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.internal.GrpcChannelOptions;

/** Abstracts away the topics gRPC configuration tunables. */
public class TopicsGrpcConfiguration implements ITopicsGrpcConfiguration {

  private final @Nonnull Duration deadline;
  private final int numStreamGrpcChannels;
  private final int numUnaryGrpcChannels;
  private final @Nullable Integer maxMessageSize;
  private final @Nullable Boolean keepAliveWithoutCalls;
  private final @Nullable Duration keepAliveTimeout;
  private final @Nullable Duration keepAliveTime;

  /**
   * Constructs a TopicsGrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   */
  public TopicsGrpcConfiguration(@Nonnull Duration deadline) {
    this(
        deadline,
        4,
        4,
        GrpcChannelOptions.DEFAULT_MAX_MESSAGE_SIZE,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_WITHOUT_STREAM,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_TIMEOUT,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_TIME);
  }

  /**
   * Constructs a TopicsGrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   * @param numStreamGrpcChannels The number of stream gRPC channels to keep open at any given time.
   * @param numUnaryGrpcChannels The number of unary gRPC channels to keep open at any given time.
   * @param maxMessageSize The maximum size of a message (in bytes) that can be received by the
   *     client.
   * @param keepAliveWithoutCalls Whether to send keepalive pings without any active calls.
   * @param keepAliveTimeout The time in milliseconds to wait for a keepalive ping response before
   *     considering the connection dead.
   * @param keepAliveTime The time in milliseconds to wait between keepalive pings.
   */
  public TopicsGrpcConfiguration(
      @Nonnull Duration deadline,
      int numStreamGrpcChannels,
      int numUnaryGrpcChannels,
      Optional<Integer> maxMessageSize,
      Optional<Boolean> keepAliveWithoutCalls,
      Optional<Integer> keepAliveTimeout,
      Optional<Integer> keepAliveTime) {
    this(
        deadline,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
        maxMessageSize.orElse(null),
        keepAliveWithoutCalls.orElse(null),
        keepAliveTimeout.map(Duration::ofMillis).orElse(null),
        keepAliveTime.map(Duration::ofMillis).orElse(null));
  }

  /**
   * Constructs a TopicsGrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   * @param numStreamGrpcChannels The number of stream gRPC channels to keep open at any given time.
   * @param numUnaryGrpcChannels The number of unary gRPC channels to keep open at any given time.
   * @param maxMessageSize The maximum size of a message (in bytes) that can be received by the
   *     client.
   * @param keepAliveWithoutCalls Whether to send keepalive pings without any active calls.
   * @param keepAliveTimeout The time to wait for a keepalive ping response before considering the
   *     connection dead.
   * @param keepAliveTime The time to wait between keepalive pings.
   */
  public TopicsGrpcConfiguration(
      @Nonnull Duration deadline,
      int numStreamGrpcChannels,
      int numUnaryGrpcChannels,
      @Nullable Integer maxMessageSize,
      @Nullable Boolean keepAliveWithoutCalls,
      @Nullable Duration keepAliveTimeout,
      @Nullable Duration keepAliveTime) {
    ensureRequestDeadlineValid(deadline);
    this.deadline = deadline;
    this.numStreamGrpcChannels = numStreamGrpcChannels;
    this.numUnaryGrpcChannels = numUnaryGrpcChannels;
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
   * @return The updated TopicsGrpcConfiguration.
   */
  public TopicsGrpcConfiguration withDeadline(Duration deadline) {
    return new TopicsGrpcConfiguration(
        deadline,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  /**
   * @deprecated This is a no-op. Use {@link #getNumStreamGrpcChannels()} (int)} and {@link
   *     #getNumUnaryGrpcChannels()} (int)} instead.
   * @return the minimum number of gRPC channels.
   */
  @Override
  public int getMinNumGrpcChannels() {
    return 0;
  }

  @Override
  public int getNumStreamGrpcChannels() {
    return numStreamGrpcChannels;
  }

  @Override
  public int getNumUnaryGrpcChannels() {
    return numUnaryGrpcChannels;
  }

  /**
   * @deprecated This is a no-op. Use {@link #withNumStreamGrpcChannels(int)} and {@link
   *     #withNumUnaryGrpcChannels(int)} instead. Copy constructor that updates the minimum number
   *     of gRPC channels.
   * @param minNumGrpcChannels The new minimum number of gRPC channels.
   * @return The updated TopicsGrpcConfiguration.
   */
  public GrpcConfiguration withMinNumGrpcChannels(int minNumGrpcChannels) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  /**
   * Copy constructor that updates the number of stream gRPC channels.
   *
   * @param numStreamGrpcChannels The new number of stream gRPC channels.
   * @return The updated TopicsGrpcConfiguration.
   */
  public TopicsGrpcConfiguration withNumStreamGrpcChannels(int numStreamGrpcChannels) {
    return new TopicsGrpcConfiguration(
        deadline,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  /**
   * Copy constructor that updates the number of unary gRPC channels.
   *
   * @param numUnaryGrpcChannels The new number of unary gRPC channels.
   * @return The updated TopicsGrpcConfiguration.
   */
  public TopicsGrpcConfiguration withNumUnaryGrpcChannels(int numUnaryGrpcChannels) {
    return new TopicsGrpcConfiguration(
        deadline,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
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
   * @return The updated TopicsGrpcConfiguration.
   */
  public TopicsGrpcConfiguration withMaxMessageSize(int maxMessageSize) {
    return new TopicsGrpcConfiguration(
        deadline,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
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
   * @return The updated TopicsGrpcConfiguration.
   */
  public TopicsGrpcConfiguration withKeepAliveWithoutCalls(
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
   * @return The updated TopicsGrpcConfiguration.
   */
  public TopicsGrpcConfiguration withKeepAliveWithoutCalls(
      @Nullable Boolean keepAliveWithoutCalls) {
    return new TopicsGrpcConfiguration(
        deadline,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
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
   * @return The updated TopicsGrpcConfiguration.
   */
  public TopicsGrpcConfiguration withKeepAliveTimeout(int keepAliveTimeoutMs) {
    return new TopicsGrpcConfiguration(
        deadline,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
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
   * @return The updated TopicsGrpcConfiguration.
   */
  public TopicsGrpcConfiguration withKeepAliveTime(int keepAliveTimeMs) {
    return new TopicsGrpcConfiguration(
        deadline,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
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
   * @return The updated TopicsGrpcConfiguration.
   */
  public TopicsGrpcConfiguration withKeepAliveDisabled() {
    return new TopicsGrpcConfiguration(
        deadline, numStreamGrpcChannels, numUnaryGrpcChannels, maxMessageSize, null, null, null);
  }
}
