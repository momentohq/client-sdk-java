package momento.sdk;

import grpc.control_client.CreateCacheRequest;
import grpc.control_client.CreateCacheResponse;
import grpc.control_client.ScsControlGrpc;
import grpc.control_client.ScsControlGrpc.ScsControlBlockingStub;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

public class Momento implements Closeable {

    private static Momento momentoInstance = null;
    private final String authToken;
    private final ScsControlBlockingStub blockingStub;
    private final ManagedChannel channel;

    public static void init(String authToken) {
        if (authToken == null) {
            throw new IllegalArgumentException("cannot pass a null authToken");
        }
        momentoInstance = new momento.sdk.Momento(authToken);
    }

    private Momento(String authToken) {
        this.authToken = authToken;
        // FIXME get endpoint from JWT claim
        NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(
                "control.cell-alpha-dev.preprod.a.momentohq.com", 443
        );
        channelBuilder.useTransportSecurity();
        channelBuilder.disableRetry();
        List<ClientInterceptor> clientInterceptors = new ArrayList<>();
        clientInterceptors.add(new AuthInterceptor(authToken));
        channelBuilder.intercept(clientInterceptors);
        ManagedChannel channel = channelBuilder.build();
        this.blockingStub = ScsControlGrpc.newBlockingStub(channel);
        this.channel = channel;
    }


    public static Cache createCache(String cacheName) {
        checkInitialized();
        checkCacheNameValid(cacheName);
        CreateCacheResponse ignored = momentoInstance.blockingStub.createCache(buildCreateCacheRequest(cacheName));
        return new Cache(momentoInstance.authToken, cacheName);
    }

    public Cache getCache(String cacheName) {
        checkInitialized();
        checkCacheNameValid(cacheName);
        return new Cache(momentoInstance.authToken, cacheName);
    }

    private static CreateCacheRequest buildCreateCacheRequest(String cacheName) {
        return CreateCacheRequest
                .newBuilder()
                .setCacheName(cacheName)
                .build();
    }

    private static void checkInitialized() {
        if (momentoInstance == null) {
            throw new RuntimeException("must initialize momento sdk with auth token");
        }
    }

    private static void checkCacheNameValid(String cacheName) {
        if (cacheName == null) {
            throw new IllegalArgumentException("null cacheName passed");
        }
    }

    public void close() {
        this.channel.shutdown();
    }

}
