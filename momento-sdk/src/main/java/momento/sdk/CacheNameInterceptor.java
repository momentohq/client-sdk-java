package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

final class CacheNameInterceptor implements ClientInterceptor {

  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);
  private final String cacheName;

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
        metadata.put(CACHE_NAME_KEY, cacheName);
        super.start(listener, metadata);
      }
    };
  }
}
