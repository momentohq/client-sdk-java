package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.time.Duration;

final class AuthInterceptor implements ClientInterceptor {

  private static final Metadata.Key<String> AUTH_HEADER_KEY =
      Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);
  private final String tokenValue;
  private Duration duration;

  AuthInterceptor(String token) {
    tokenValue = token;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        channel.newCall(methodDescriptor, callOptions)) {
      @Override
      public void start(Listener<RespT> listener, Metadata metadata) {
        metadata.put(AUTH_HEADER_KEY, tokenValue);
        super.start(listener, metadata);
      }
    };
  }
}
