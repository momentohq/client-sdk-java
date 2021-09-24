package momento.sdk;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

// TODO: This should be made package default
public class CacheNameInterceptor implements ClientInterceptor {

    private Metadata.Key<String> cacheNameKey = Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);
    private String cacheName;

    public CacheNameInterceptor(String inputCacheName) {
        cacheName = inputCacheName;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(Listener<RespT> listener, Metadata metadata) {
                metadata.put(cacheNameKey, cacheName);
                super.start(listener, metadata);
            }
        };
    }
}
