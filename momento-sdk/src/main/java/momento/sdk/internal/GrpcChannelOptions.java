package momento.sdk.internal;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import momento.sdk.config.transport.GrpcConfiguration;

public class GrpcChannelOptions {
  // The default value for max_send_message_length is 4mb.  We need to increase this to 5mb in order
  // to
  // support cases where users have requested a limit increase up to our maximum item size of 5mb.
  public static final int DEFAULT_MAX_MESSAGE_SIZE = 5_243_000; // bytes

  public static final boolean DEFAULT_KEEPALIVE_WITHOUT_STREAM = true;
  public static final int DEFAULT_KEEPALIVE_TIME_MS = 5000; // milliseconds
  public static final int DEFAULT_KEEPALIVE_TIMEOUT_MS = 1000; // milliseconds

  public static void applyGrpcConfigurationToChannelBuilder(
      GrpcConfiguration grpcConfig, NettyChannelBuilder channelBuilder) {
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();

    final Optional<Integer> maxMessageSize = grpcConfig.getMaxMessageSize();
    if (maxMessageSize.isPresent()) {
      channelBuilder.maxInboundMessageSize(maxMessageSize.get());
    }

    // no equivalent for maxOutboundboundMessageSize

    final Optional<Integer> keepAliveTimeMs = grpcConfig.getKeepAliveTimeMs();
    if (keepAliveTimeMs.isPresent()) {
      channelBuilder.keepAliveTime(keepAliveTimeMs.get(), TimeUnit.MILLISECONDS);
    }

    final Optional<Integer> keepAliveTimeoutMs = grpcConfig.getKeepAliveTimeoutMs();
    if (keepAliveTimeoutMs.isPresent()) {
      channelBuilder.keepAliveTimeout(keepAliveTimeoutMs.get(), TimeUnit.MILLISECONDS);
    }

    final Optional<Boolean> keepAliveWithoutCalls = grpcConfig.getKeepAliveWithoutCalls();
    if (keepAliveWithoutCalls.isPresent()) {
      channelBuilder.keepAliveWithoutCalls(keepAliveWithoutCalls.get());
    }
  }
}
