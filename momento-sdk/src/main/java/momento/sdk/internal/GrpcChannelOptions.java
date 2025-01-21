package momento.sdk.internal;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import momento.sdk.config.transport.IGrpcConfiguration;

public class GrpcChannelOptions {
  // The default value for max_send_message_length is 4MB.  We need to increase this to 5MB in order
  // to support cases where users have requested a limit increase up to our maximum item size of
  // 5MB.
  public static final int DEFAULT_MAX_MESSAGE_SIZE = 5_243_000; // bytes
  // Leaderboards have a separate max message size from the cache methods. This default limit of
  // 200MB is in place to prevent memory issues in the event of an erroneously large message.
  public static final int DEFAULT_LEADERBOARD_MAX_MESSAGE_SIZE = 209_715_200; // bytes

  public static final boolean DEFAULT_KEEPALIVE_WITHOUT_STREAM = true;
  public static final int DEFAULT_KEEPALIVE_TIME_MS = 5000; // milliseconds
  public static final Duration DEFAULT_KEEPALIVE_TIME =
      Duration.ofMillis(DEFAULT_KEEPALIVE_TIME_MS);
  public static final int DEFAULT_KEEPALIVE_TIMEOUT_MS = 1000; // milliseconds
  public static final Duration DEFAULT_KEEPALIVE_TIMEOUT =
      Duration.ofMillis(DEFAULT_KEEPALIVE_TIMEOUT_MS);

  public static void applyGrpcConfigurationToChannelBuilder(
          IGrpcConfiguration grpcConfig, NettyChannelBuilder channelBuilder) {
    applyGrpcConfigurationToChannelBuilder(grpcConfig, channelBuilder, true);
  }

  public static void applyGrpcConfigurationToChannelBuilder(
      IGrpcConfiguration grpcConfig, NettyChannelBuilder channelBuilder, boolean isSecure) {
    if (isSecure) {
      channelBuilder.useTransportSecurity();
      channelBuilder.disableRetry();
    } else {
      channelBuilder.usePlaintext();
      channelBuilder.enableRetry();
      channelBuilder.maxRetryAttempts(3);
    }

    grpcConfig.getMaxReceivedMessageSize().ifPresent(channelBuilder::maxInboundMessageSize);

    // no equivalent for maxOutboundMessageSize

    grpcConfig
        .getKeepAliveTime()
        .ifPresent(d -> channelBuilder.keepAliveTime(d.toMillis(), TimeUnit.MILLISECONDS));

    grpcConfig
        .getKeepAliveTimeout()
        .ifPresent(d -> channelBuilder.keepAliveTimeout(d.toMillis(), TimeUnit.MILLISECONDS));

    grpcConfig.getKeepAliveWithoutCalls().ifPresent(channelBuilder::keepAliveWithoutCalls);
  }
}
