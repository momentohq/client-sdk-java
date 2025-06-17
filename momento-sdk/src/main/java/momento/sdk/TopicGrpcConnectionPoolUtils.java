package momento.sdk;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfiguration;
import momento.sdk.config.middleware.Middleware;
import momento.sdk.config.middleware.MiddlewareRequestHandlerContext;
import momento.sdk.internal.GrpcChannelOptions;

// Utility class for setting up a connection to the Momento Topic service.
final class TopicGrpcConnectionPoolUtils {

  // Set up a connection to the Momento Topic service.
  protected static ManagedChannel setupConnection(
      CredentialProvider credentialProvider,
      TopicConfiguration configuration,
      UUID connectionIdKey) {
    final NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(
            credentialProvider.getCacheEndpoint(), credentialProvider.getPort());

    // set additional channel options (message size, keepalive, auth, etc)
    GrpcChannelOptions.applyGrpcConfigurationToChannelBuilder(
        configuration.getTransportStrategy().getGrpcConfiguration(),
        channelBuilder,
        credentialProvider.isEndpointSecure());

    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();

    final List<Middleware> middlewares = configuration.getMiddlewares();
    final MiddlewareRequestHandlerContext context =
        () -> Collections.singletonMap(connectionIdKey.toString(), UUID.randomUUID().toString());
    clientInterceptors.add(new GrpcMiddlewareInterceptor(middlewares, context));

    clientInterceptors.add(new UserHeaderInterceptor(credentialProvider.getAuthToken(), "topic"));
    channelBuilder.intercept(clientInterceptors);
    return channelBuilder.build();
  }
}
