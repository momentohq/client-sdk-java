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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

abstract class ClientBase implements AutoCloseable {
  protected <S extends AbstractFutureStub<S>> S attachMetadata(S stub, Metadata metadata) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  protected Metadata metadataWithItem(Metadata.Key<String> key, String value) {
    final Metadata metadata = new Metadata();
    metadata.put(key, value);
    return metadata;
  }

  protected <SdkResponse, GrpcResponse> CompletableFuture<SdkResponse> executeGrpcFunction(
      Supplier<ListenableFuture<GrpcResponse>> stubSupplier,
      Function<GrpcResponse, SdkResponse> successFunction,
      Function<Throwable, SdkResponse> errorFunction) {

    // Submit request to non-blocking stub
    final ListenableFuture<GrpcResponse> rspFuture = stubSupplier.get();

    // Build a CompletableFuture to return to caller
    final CompletableFuture<SdkResponse> returnFuture =
        new CompletableFuture<SdkResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
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
  }

  protected <SdkResponse, GrpcResponse> CompletableFuture<SdkResponse> executeGrpcBatchFunction(
      Consumer<StreamObserver<GrpcResponse>> stubMethod,
      Function<List<GrpcResponse>, SdkResponse> successFunction,
      Function<Throwable, SdkResponse> errorFunction) {

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
  }
}
