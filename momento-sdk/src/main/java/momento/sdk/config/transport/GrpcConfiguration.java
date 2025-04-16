package momento.sdk.config.transport;

import static momento.sdk.ValidationUtils.ensureRequestDeadlineValid;

import java.time.Duration;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.internal.GrpcChannelOptions;

/** Abstracts away the gRPC configuration tunables. */
public class GrpcConfiguration implements IGrpcConfiguration {

  private final @Nonnull Duration deadline;
  private final int minNumGrpcChannels;
  private final @Nullable Integer numStreamGrpcChannels;
  private final @Nullable Integer numUnaryGrpcChannels;
  private final @Nullable Integer maxMessageSize;
  private final @Nullable Boolean keepAliveWithoutCalls;
  private final @Nullable Duration keepAliveTimeout;
  private final @Nullable Duration keepAliveTime;

  /**
   * Constructs a GrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   */
  public GrpcConfiguration(@Nonnull Duration deadline) {
    this(
        deadline,
        GrpcChannelOptions.DEFAULT_NUM_GRPC_CHANNELS,
        null,
        null,
        GrpcChannelOptions.DEFAULT_MAX_MESSAGE_SIZE,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_WITHOUT_STREAM,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_TIMEOUT,
        GrpcChannelOptions.DEFAULT_KEEPALIVE_TIME);
  }

  /**
   * Constructs a GrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   * @param minNumGrpcChannels The minimum number of gRPC channels to keep open at any given time.
   * @param numStreamGrpcChannels The number of stream grpc channels to keep open at any given time.
   * @param numUnaryGrpcChannels The number of unary grpc channels to keep open at any given time.
   * @param maxMessageSize The maximum size of a message (in bytes) that can be received by the
   *     client.
   * @param keepAliveWithoutCalls Whether to send keepalive pings without any active calls.
   * @param keepAliveTimeout The time in milliseconds to wait for a keepalive ping response before
   *     considering the connection dead.
   * @param keepAliveTime The time in milliseconds to wait between keepalive pings.
   */
  public GrpcConfiguration(
      @Nonnull Duration deadline,
      int minNumGrpcChannels,
      Optional<Integer> numStreamGrpcChannels,
      Optional<Integer> numUnaryGrpcChannels,
      Optional<Integer> maxMessageSize,
      Optional<Boolean> keepAliveWithoutCalls,
      Optional<Integer> keepAliveTimeout,
      Optional<Integer> keepAliveTime) {
    this(
        deadline,
        minNumGrpcChannels,
        numStreamGrpcChannels.orElse(null),
        numUnaryGrpcChannels.orElse(null),
        maxMessageSize.orElse(null),
        keepAliveWithoutCalls.orElse(null),
        keepAliveTimeout.map(Duration::ofMillis).orElse(null),
        keepAliveTime.map(Duration::ofMillis).orElse(null));
  }

  /**
   * Constructs a GrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   * @param minNumGrpcChannels The minimum number of gRPC channels to keep open at any given time.
   * @param numStreamGrpcChannels The number of stream grpc channels to keep open at any given time.
   * @param numUnaryGrpcChannels The number of unary grpc channels to keep open at any given time.
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
      @Nullable Integer numStreamGrpcChannels,
      @Nullable Integer numUnaryGrpcChannels,
      @Nullable Integer maxMessageSize,
      @Nullable Boolean keepAliveWithoutCalls,
      @Nullable Duration keepAliveTimeout,
      @Nullable Duration keepAliveTime) {
    ensureRequestDeadlineValid(deadline);
    this.deadline = deadline;
    this.minNumGrpcChannels = minNumGrpcChannels;
    this.numStreamGrpcChannels = numStreamGrpcChannels;
    this.numUnaryGrpcChannels = numUnaryGrpcChannels;
    this.maxMessageSize = maxMessageSize;
    this.keepAliveWithoutCalls = keepAliveWithoutCalls;
    this.keepAliveTimeout = keepAliveTimeout;
    this.keepAliveTime = keepAliveTime;
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
      @Nullable Integer maxMessageSize,
      @Nullable Boolean keepAliveWithoutCalls,
      @Nullable Duration keepAliveTimeout,
      @Nullable Duration keepAliveTime) {
    ensureRequestDeadlineValid(deadline);
    this.deadline = deadline;
    this.minNumGrpcChannels = minNumGrpcChannels;
    this.numStreamGrpcChannels = null;
    this.numUnaryGrpcChannels = null;
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
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withDeadline(Duration deadline) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
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
   * Gets the explicitly configured number of streaming gRPC channels, or fallback to
   * minNumGrpcChannels.
   *
   * @return The number of streaming gRPC channels to create.
   */
  public int getNumStreamGrpcChannels() {
    return numStreamGrpcChannels != null ? numStreamGrpcChannels : minNumGrpcChannels;
  }

  /**
   * Gets the explicitly configured number of unary gRPC channels, or fallback to
   * minNumGrpcChannels.
   *
   * @return The number of unary gRPC channels to create.
   */
  public int getNumUnaryGrpcChannels() {
    return numUnaryGrpcChannels != null ? numUnaryGrpcChannels : minNumGrpcChannels;
  }

  /**
   * Copy constructor that updates the minimum number of gRPC channels. <b>Note:</b> This setting is
   * ignored by the TopicClient. Use {@link #withNumStreamGrpcChannels} and {@link
   * #withNumUnaryGrpcChannels} instead for topic clients.
   *
   * @param minNumGrpcChannels The new minimum number of gRPC channels.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withMinNumGrpcChannels(int minNumGrpcChannels) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  /**
   * Copy constructor that sets the number of stream gRPC channels to be used by clients that
   * support streaming (e.g. TopicClient). <b>Note:</b> This setting is ignored by the CacheClient.
   *
   * @param numStreamGrpcChannels The new number of streaming gRPC channels.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withNumStreamGrpcChannels(int numStreamGrpcChannels) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
        maxMessageSize,
        keepAliveWithoutCalls,
        keepAliveTimeout,
        keepAliveTime);
  }

  /**
   * Copy constructor that sets the number of unary gRPC channels. <b>Note:</b> This setting is
   * ignored by the CacheClient.
   *
   * @param numUnaryGrpcChannels The new minimum number of gRPC channels.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withNumUnaryGrpcChannels(int numUnaryGrpcChannels) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
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
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withMaxMessageSize(int maxMessageSize) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
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
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withKeepAliveWithoutCalls(Optional<Boolean> keepAliveWithoutCalls) {
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
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withKeepAliveWithoutCalls(@Nullable Boolean keepAliveWithoutCalls) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
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
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withKeepAliveTimeout(int keepAliveTimeoutMs) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
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
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withKeepAliveTime(int keepAliveTimeMs) {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
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
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withKeepAliveDisabled() {
    return new GrpcConfiguration(
        deadline,
        minNumGrpcChannels,
        numStreamGrpcChannels,
        numUnaryGrpcChannels,
        maxMessageSize,
        null,
        null,
        null);
  }
}
