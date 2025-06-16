package momento.sdk.interceptors;

import com.google.protobuf.Message;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ConnectivityState;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import momento.sdk.config.middleware.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GrpcMiddlewareInterceptor implements ClientInterceptor {

  private final List<MiddlewareRequestHandler> middlewareHandlers;
  private final Logger logger = LoggerFactory.getLogger(GrpcMiddlewareInterceptor.class);

  public GrpcMiddlewareInterceptor(
      List<Middleware> middlewares, MiddlewareRequestHandlerContext context) {
    this.middlewareHandlers =
        middlewares.stream()
            .map(middleware -> middleware.onNewRequest(context))
            .collect(Collectors.toList());
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      final MethodDescriptor<ReqT, RespT> methodDescriptor,
      final CallOptions callOptions,
      final Channel channel) {

    final ClientCall<ReqT, RespT> delegateCall = channel.newCall(methodDescriptor, callOptions);

    return new ForwardingClientCall<ReqT, RespT>() {
      @Override
      public void start(
          final ClientCall.Listener<RespT> responseListener, final Metadata metadata) {
        processMiddleware(
                new MiddlewareMetadata(metadata),
                middlewareHandlers,
                MiddlewareRequestHandler::onRequestMetadata)
            .thenAccept(
                updatedMetadata ->
                    delegateCall.start(
                        new MiddlewareResponseListener<>(
                            responseListener, channel, methodDescriptor),
                        updatedMetadata.getGrpcMetadata()));
      }

      @Override
      public void sendMessage(final ReqT message) {
        if (message instanceof Message) {
          final Message protoMessage = (Message) message;
          processMiddleware(
                  new MiddlewareMessage(protoMessage),
                  middlewareHandlers,
                  MiddlewareRequestHandler::onRequestBody)
              .thenAccept(
                  updatedMessage -> delegateCall.sendMessage((ReqT) updatedMessage.getMessage()));
        } else {
          delegateCall.sendMessage(message);
        }
      }

      @Override
      protected ClientCall<ReqT, RespT> delegate() {
        return delegateCall;
      }
    };
  }

  private static <T> CompletableFuture<T> processMiddleware(
      T initial, List<MiddlewareRequestHandler> handlers, final MiddlewareProcessor<T> processor) {
    CompletableFuture<T> future = CompletableFuture.completedFuture(initial);
    for (final MiddlewareRequestHandler handler : handlers) {
      future =
          future.thenCompose(
              (Function<T, CompletableFuture<T>>) input -> processor.apply(handler, input));
    }
    return future;
  }

  private class MiddlewareResponseListener<RespT>
      extends ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT> {
    private final Channel channel;
    private final MethodDescriptor<?, ?> methodDescriptor;

    protected MiddlewareResponseListener(
        ClientCall.Listener<RespT> delegate,
        Channel channel,
        MethodDescriptor<?, ?> methodDescriptor) {
      super(delegate);
      this.channel = channel;
      this.methodDescriptor = methodDescriptor;
    }

    @Override
    public void onHeaders(final Metadata headers) {
      processMiddleware(
              new MiddlewareMetadata(headers),
              middlewareHandlers,
              (handler, input) -> handler.onResponseMetadata(input))
          .thenAccept(
              updatedHeaders ->
                  MiddlewareResponseListener.super.onHeaders(updatedHeaders.getGrpcMetadata()));
    }

    @Override
    public void onMessage(final RespT message) {
      if (message instanceof Message) {
        final Message protoMessage = (Message) message;
        processMiddleware(
                new MiddlewareMessage(protoMessage),
                middlewareHandlers,
                MiddlewareRequestHandler::onResponseBody)
            .thenAccept(
                updatedMessage ->
                    MiddlewareResponseListener.super.onMessage(
                        (RespT) updatedMessage.getMessage()));
      } else {
        super.onMessage(message);
      }
    }

    @Override
    public void onClose(final Status status, final Metadata trailers) {
      if (status.getCode() == Status.Code.DEADLINE_EXCEEDED && channel instanceof ManagedChannel) {
        ConnectivityState connectionStatus = ((ManagedChannel) channel).getState(false);
        logger.warn(
            "gRPC Deadline Exceeded: {} - {} | Connection state: {} | Method: {}",
            status.getCode(),
            status.getDescription(),
            connectionStatus,
            methodDescriptor.getFullMethodName());
      }

      processMiddleware(
              new MiddlewareStatus(status),
              middlewareHandlers,
              MiddlewareRequestHandler::onResponseStatus)
          .thenAccept(
              updatedStatus ->
                  MiddlewareResponseListener.super.onClose(
                      updatedStatus.getGrpcStatus(), trailers));
    }
  }

  private interface MiddlewareProcessor<T> {
    CompletableFuture<T> apply(MiddlewareRequestHandler handler, T input);
  }
}
