package momento.sdk;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Metadata;
import io.grpc.stub.AbstractFutureStub;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ClientBase implements AutoCloseable {

  protected final ExecutorService requestConcurrencyExecutor;

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public ClientBase(@Nullable Integer concurrencyLimit) {
    if (concurrencyLimit != null) {
      requestConcurrencyExecutor = Executors.newFixedThreadPool(concurrencyLimit);
    } else {
      requestConcurrencyExecutor = null;
    }
  }

  protected <S extends AbstractFutureStub<S>> S attachMetadata(S stub, Metadata metadata) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  protected Metadata metadataWithItem(Metadata.Key<String> key, String value) {
    final Metadata metadata = new Metadata();
    metadata.put(key, value);
    return metadata;
  }

  /**
   * Executes the provided operation using the request concurrency executor in order to limit the
   * number of concurrent operations to the size of the executor thread pool. Executes the operation
   * directly if the executor is null.
   */
  protected <R> CompletableFuture<R> executeWithConcurrencyLimiting(
      Supplier<CompletableFuture<R>> operation, Function<Throwable, R> errorHandler) {

    if (requestConcurrencyExecutor != null) {
      return CompletableFuture.supplyAsync(
          () -> {
            try {
              // The first get() starts the operation and the second get() blocks the executor
              // thread until the operation completes. The result is then wrapped in the outer
              // CompletableFuture.
              return operation.get().get();
            } catch (Exception e) {
              return errorHandler.apply(e);
            }
          },
          requestConcurrencyExecutor);
    } else {
      return operation.get();
    }
  }

  protected <SdkResponse, GrpcResponse> CompletableFuture<SdkResponse> executeGrpcFunction(
      Supplier<ListenableFuture<GrpcResponse>> stubSupplier,
      Function<GrpcResponse, SdkResponse> successFunction,
      Function<Throwable, SdkResponse> errorFunction) {

    return executeWithConcurrencyLimiting(
        () -> {
          // Submit request to non-blocking stub
          final ListenableFuture<GrpcResponse> rspFuture = stubSupplier.get();

          // Build a CompletableFuture to return to caller
          final CompletableFuture<SdkResponse> returnFuture =
              new CompletableFuture<SdkResponse>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                  // propagate cancel to the listenable future if called on returned completable
                  // future
                  final boolean result = rspFuture.cancel(mayInterruptIfRunning);
                  super.cancel(mayInterruptIfRunning);
                  return result;
                }
              };

          // Convert returned ListenableFuture to CompletableFuture
          Futures.addCallback(
              rspFuture,
              new FutureCallback<GrpcResponse>() {
                @Override
                public void onSuccess(GrpcResponse rsp) {
                  returnFuture.complete(successFunction.apply(rsp));
                }

                @Override
                public void onFailure(@Nonnull Throwable e) {
                  returnFuture.complete(errorFunction.apply(e));
                }
              },
              // Execute on same thread that called execute on CompletionStage
              MoreExecutors.directExecutor());

          return returnFuture;
        },
        errorFunction);
  }

  protected <SdkResponse, GrpcResponse> CompletableFuture<SdkResponse> executeGrpcBatchFunction(
      Consumer<StreamObserver<GrpcResponse>> stubMethod,
      Function<List<GrpcResponse>, SdkResponse> successFunction,
      Function<Throwable, SdkResponse> errorFunction) {

    return executeWithConcurrencyLimiting(
        () -> {
          final CompletableFuture<SdkResponse> future = new CompletableFuture<>();
          try {
            stubMethod.accept(
                new StreamObserver<GrpcResponse>() {
                  private final List<GrpcResponse> responses = new ArrayList<>();

                  @Override
                  public void onNext(GrpcResponse response) {
                    responses.add(response);
                  }

                  @Override
                  public void onError(Throwable t) {
                    future.complete(errorFunction.apply(t));
                  }

                  @Override
                  public void onCompleted() {
                    future.complete(successFunction.apply(responses));
                  }
                });
          } catch (Exception e) {
            future.complete(errorFunction.apply(e));
          }

          return future;
        },
        errorFunction);
  }

  /**
   * Closes this resource, relinquishing any underlying resources. This method is invoked in the
   * close method of the base client class.
   */
  public abstract void doClose();

  /**
   * Gracefully shuts down the request concurrency executor, if one exists. This happens ahead of
   * shutting down the actual gRPC clients, so it acts mainly to let any queued up requests get
   * through.
   */
  private void closeRequestConcurrencyExecutor() {
    if (requestConcurrencyExecutor != null) {
      try {
        requestConcurrencyExecutor.shutdown();
        if (!requestConcurrencyExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          logger.warn(
              "Momento requests still processing after 30 seconds while awaiting shutdown.");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() {
    closeRequestConcurrencyExecutor();
    doClose();
  }
}
