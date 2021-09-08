package org.momento.client;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;

// TODO: This should be made package default
public class OpenTelemetryClientInterceptor implements ClientInterceptor {

    private final TextMapPropagator textFormat;

    public OpenTelemetryClientInterceptor(OpenTelemetry openTelemetry) {
        this.textFormat = openTelemetry.getPropagators().getTextMapPropagator();
    }
    private final TextMapSetter<Metadata> setter =
            (carrier, key, value) ->
                    carrier.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall(
                channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(Listener responseListener, Metadata headers) {
                // Inject the request with the current context
                textFormat.inject(io.opentelemetry.context.Context.current(), headers, setter);
                // Perform the gRPC request
                super.start(responseListener, headers);
            }
        };
    }
}
