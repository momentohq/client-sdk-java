package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Manager responsible for GRPC channels and stubs for the Topics.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsTopicGrpcStubsManager implements Closeable {

    private final Duration deadline;

    private final ManagedChannel channel;

//    private final PubsubGrpc.PubsubFutureStub futureStub;

    private final PubsubGrpc.PubsubStub stub;
    ScsTopicGrpcStubsManager(@Nonnull CredentialProvider credentialProvider, @Nonnull Configuration configuration) {
        this.channel = setupConnection(credentialProvider);
//        this.futureStub = PubsubGrpc.newFutureStub(channel);
        this.stub = PubsubGrpc.newStub(channel);
        this.deadline = configuration.getTransportStrategy().getGrpcConfiguration().getDeadline();
    }

    private static ManagedChannel setupConnection(CredentialProvider credentialProvider) {
        final NettyChannelBuilder channelBuilder =
                NettyChannelBuilder.forAddress(credentialProvider.getCacheEndpoint(), 443);
        channelBuilder.useTransportSecurity();
        channelBuilder.disableRetry();
        final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
        clientInterceptors.add(new UserHeaderInterceptor(credentialProvider.getAuthToken()));
        channelBuilder.intercept(clientInterceptors);
        return channelBuilder.build();
    }

    /**
     * Returns a stub with appropriate deadlines.
     *
     * <p>Each stub is deliberately decorated with Deadline. Deadlines work differently than timeouts.
     * When a deadline is set on a stub, it simply means that once the stub is created it must be used
     * before the deadline expires. Hence, the stub returned from here should never be cached and the
     * safest behavior is for clients to request a new stub each time.
     *
     * <p><a href="https://github.com/grpc/grpc-java/issues/1495">more information</a>
     */
//    PubsubGrpc.PubsubFutureStub getStub() {
//        return futureStub.withDeadlineAfter(deadline.getSeconds(), TimeUnit.SECONDS);
//    }

    PubsubGrpc.PubsubStub getStub() {
        return stub.withDeadlineAfter(deadline.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        channel.shutdown();
    }
}
