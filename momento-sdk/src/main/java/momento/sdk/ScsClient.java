package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Metadata;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractFutureStub;
import io.grpc.stub.MetadataUtils;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

abstract class ScsClient implements AutoCloseable {

  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);

  protected Metadata metadataWithCache(String cacheName) {
    final Metadata metadata = new Metadata();
    metadata.put(CACHE_NAME_KEY, cacheName);

    return metadata;
  }

  protected <S extends AbstractFutureStub<S>> S attachMetadata(S stub, Metadata metadata) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  protected <S extends AbstractBlockingStub<S>> S attachMetadataBlocking(
      S stub, Metadata metadata) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
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
}
