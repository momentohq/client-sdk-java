package momento.sdk.internal;

import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
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

  public static NettyChannelBuilder GrpcOptionsFromGrpcConfig(
      GrpcConfiguration grpcConfig, NettyChannelBuilder channelBuilder) {
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();
    channelBuilder.maxInboundMessageSize(grpcConfig.getMaxMessageSize());
    // no equivalent for maxOutboundboundMessageSize

    if (grpcConfig.getKeepAliveTimeMs() > 0) {
      channelBuilder.keepAliveTime(grpcConfig.getKeepAliveTimeMs(), TimeUnit.MILLISECONDS);
    }

    if (grpcConfig.getKeepAliveTimeoutMs() > 0) {
      channelBuilder.keepAliveTimeout(grpcConfig.getKeepAliveTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    if (!grpcConfig.getKeepAliveWithoutCalls()) {
      channelBuilder.keepAliveWithoutCalls(false);
    }

    return channelBuilder;
  }
}
