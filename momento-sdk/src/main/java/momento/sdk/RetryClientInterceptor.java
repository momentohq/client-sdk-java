package momento.sdk;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import momento.sdk.retry.RetryEligibilityStrategy;
import momento.sdk.retry.RetryStrategy;
import momento.sdk.retry.RetryingUnaryClientCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interceptor for retrying client calls with gRPC servers. This interceptor is responsible for
 * handling retry logic when making unary (single request, single response) gRPC calls.
 *
 * <p>A {@link ClientCall} is essentially an instance of a gRPC invoker. Every gRPC interceptor
 * expects us to return such client call(s) that it will execute in order. Each call has a "start"
 * method, which is the entry point for the call.
 *
 * <p>This retry client interceptor returns an instance of a {@link RetryingUnaryClientCall}, which
 * is a client call designed to handle retrying unary (single request, single response) operations.
 * The interceptor uses a provided {@link RetryStrategy} to determine when and how to retry failed
 * calls.
 *
 * <p>When a gRPC call is intercepted, the interceptor checks whether the method is unary (client
 * sends one message), and if so, it wraps the original {@link ClientCall} with the {@link
 * RetryingUnaryClientCall}. This custom call is responsible for handling the retry logic.
 *
 * <p>When the gRPC call is closed, the {@code onClose} method is called, which is the point where
 * we can safely check the status of the initial request that was made and determine if we want to
 * retry or not. It starts the retrying process by scheduling a new call attempt with a delay
 * provided by the {@link RetryStrategy}.
 *
 * <p>If the retry attempts are exhausted or if the provided delay is not present (indicating that
 * we should not retry anymore), the interceptor propagates the final result to the original
 * listener, effectively completing the call with the last status received.
 *
 * <p>Note that the interceptor only supports unary operations for retrying.
 *
 * @see RetryStrategy
 * @see RetryEligibilityStrategy
 * @param <ReqT> The type of the request message.
 * @param <RespT> The type of the response message.
 */
final class RetryClientInterceptor implements ClientInterceptor {

  private final RetryStrategy retryStrategy;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService executor;
  private final Logger logger = LoggerFactory.getLogger(RetryClientInterceptor.class);

  public RetryClientInterceptor(
      final RetryStrategy retryStrategy,
      final ScheduledExecutorService scheduler,
      final ExecutorService executor) {
    this.retryStrategy = retryStrategy;
    this.scheduler = scheduler;
    this.executor = executor;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      final MethodDescriptor<ReqT, RespT> method,
      final CallOptions callOptions,
      final Channel channel) {
    // currently the SDK only supports unary operations which we want to retry on
    if (!method.getType().clientSendsOneMessage()) {
      return channel.newCall(method, callOptions);
    }

    return new RetryingUnaryClientCall<ReqT, RespT>(channel.newCall(method, callOptions)) {
      private int attemptNumber = 0;
      @Nullable private Future<?> future = null;

      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        super.start(
            new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                responseListener) {

              /**
               * At this point, the ClientCall has been closed. Any additional calls to the
               * ClientCall will not be processed by the server. The server does not send any
               * further messages, acknowledgements, or notifications. This is the point where we
               * can safely check the status of the initial request that was made, and determine if
               * we want to retry or not.
               *
               * @param status the result of the remote call.
               * @param trailers metadata provided at call completion.
               */
              @Override
              public void onClose(Status status, Metadata trailers) {
                // we don't have any more business with the server, and since we either complete the
                // call or retry
                // later on, we try to cancel the current attempt. If the request was successful,
                // this
                // cancellation will be moot either way.
                cancelAttempt();

                // anything other than an OK status means it's an erroneous situation.
                // OK indicates the gRPC call completed successfully and hence we return
                if (status.isOk()) {
                  super.onClose(status, trailers);
                  return;
                }

                // now we can safely start retrying

                attemptNumber++;
                final Optional<Duration> retryDelay =
                    retryStrategy.determineWhenToRetry(status, method, attemptNumber);

                // a delay not present indicates we have exhausted retries or exceeded
                // delay or any variable the strategy author wishes to not retry anymore
                if (!retryDelay.isPresent()) {
                  super.onClose(status, trailers);
                  return;
                }

                logger.debug(
                    "Retrying request {} on error code {} with delay {} millisecodns",
                    method.getFullMethodName(),
                    status.getCode().toString(),
                    retryDelay.get().toMillis());

                final Runnable runnable =
                    Context.current().wrap(() -> retry(channel.newCall(method, callOptions)));

                // schedule the task to be executed on the executor
                future =
                    scheduler.schedule(
                        () -> executor.submit(runnable),
                        retryDelay.get().toMillis(),
                        TimeUnit.MILLISECONDS);
              }

              @Override
              public void onMessage(RespT message) {
                super.onMessage(message);
              }
            },
            headers);
      }

      @Override
      public void cancel(@Nullable String message, @Nullable Throwable cause) {
        cancelAttempt();
        super.cancel(message, cause);
      }

      private void cancelAttempt() {
        if (future != null) {
          future.cancel(true);
        }
      }
    };
  }
}
