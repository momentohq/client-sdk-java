package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.util.Collections;
import java.util.Map;

final class UserHeaderInterceptor implements ClientInterceptor {

  private static final Metadata.Key<String> AUTH_HEADER_KEY =
      Metadata.Key.of("authorization", ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> SDK_AGENT_KEY =
      Metadata.Key.of("agent", ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> RUNTIME_VERSION_KEY =
      Metadata.Key.of("runtime-version", ASCII_STRING_MARSHALLER);
  static final Metadata.Key<String> READ_CONCERN =
      Metadata.Key.of("read-concern", ASCII_STRING_MARSHALLER);

  private final Map<Metadata.Key<String>, String> extraHeaders;
  private final String tokenValue;
  private final String sdkVersion;
  private final String runtimeVersion;
  private boolean isUserAgentSent = false;

  UserHeaderInterceptor(String token, String clientType) {
    this(token, clientType, Collections.emptyMap());
  }

  UserHeaderInterceptor(
      String token, String clientType, Map<Metadata.Key<String>, String> extraHeaders) {
    tokenValue = token;
    sdkVersion =
        String.format(
            "java:%s:%s", clientType, this.getClass().getPackage().getImplementationVersion());
    runtimeVersion = System.getProperty("java.vendor") + ", " + System.getProperty("java.version");
    this.extraHeaders = extraHeaders;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        channel.newCall(methodDescriptor, callOptions)) {
      @Override
      public void start(Listener<RespT> listener, Metadata metadata) {
        metadata.put(AUTH_HEADER_KEY, tokenValue);
        if (!isUserAgentSent) {
          metadata.put(SDK_AGENT_KEY, sdkVersion);
          metadata.put(RUNTIME_VERSION_KEY, runtimeVersion);
          isUserAgentSent = true;
        }
        extraHeaders.forEach(metadata::put);
        super.start(listener, metadata);
      }
    };
  }
}
