package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static java.time.Instant.now;
import static momento.sdk.ValidationUtils.checkCacheNameValid;
import static momento.sdk.ValidationUtils.ensureValidCacheSet;
import static momento.sdk.ValidationUtils.ensureValidKey;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import grpc.cache_client.*;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.ImplicitContextKeyed;
import io.opentelemetry.context.Scope;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.messages.CacheDeleteResponse;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;

/** Client for interacting with Scs Data plane. */
final class ScsDataClient implements Closeable {

  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);

  private final Optional<Tracer> tracer;
  private final long itemDefaultTtlSeconds;
  private final ScsDataGrpcStubsManager scsDataGrpcStubsManager;
  private final String endpoint;

  ScsDataClient(
      String authToken,
      String endpoint,
      long defaultTtlSeconds,
      Optional<OpenTelemetry> openTelemetry,
      Optional<Duration> requestTimeout) {
    this.tracer = openTelemetry.map(ot -> ot.getTracer("momento-java-scs-client", "1.0.0"));
    this.itemDefaultTtlSeconds = defaultTtlSeconds;
    this.scsDataGrpcStubsManager =
        new ScsDataGrpcStubsManager(authToken, endpoint, openTelemetry, requestTimeout);
    this.endpoint = endpoint;
  }

  public String getEndpoint() {
    return endpoint;
  }

  CompletableFuture<CacheGetResponse> getAsync(String cacheName, byte[] key) {
    ensureValidKey(key);
    return sendGet(cacheName, convert(key));
  }

  CompletableFuture<CacheGetResponse> getAsync(String cacheName, String key) {
    ensureValidKey(key);
    return sendGet(cacheName, convert(key));
  }

  CompletableFuture<CacheDeleteResponse> deleteAsync(String cacheName, byte[] key) {
    ensureValidKey(key);
    return sendDelete(cacheName, convert(key));
  }

  CompletableFuture<CacheDeleteResponse> deleteAsync(String cacheName, String key) {
    ensureValidKey(key);
    return sendDelete(cacheName, convert(key));
  }

  CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, String key, ByteBuffer value, long ttlSeconds) {
    ensureValidCacheSet(key, value, ttlSeconds);
    return sendSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(String cacheName, String key, ByteBuffer value) {
    return setAsync(cacheName, key, value, itemDefaultTtlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, byte[] key, byte[] value, long ttlSeconds) {
    ensureValidCacheSet(key, value, ttlSeconds);
    return sendSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(String cacheName, byte[] key, byte[] value) {
    return setAsync(cacheName, key, value, itemDefaultTtlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, String key, String value, long ttlSeconds) {
    ensureValidCacheSet(key, value, ttlSeconds);
    return sendSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(String cacheName, String key, String value) {
    return setAsync(cacheName, key, value, itemDefaultTtlSeconds);
  }

  private ByteString convert(String stringToEncode) {
    return ByteString.copyFromUtf8(stringToEncode);
  }

  private ByteString convert(byte[] bytes) {
    return ByteString.copyFrom(bytes);
  }

  private ByteString convert(ByteBuffer byteBuffer) {
    return ByteString.copyFrom(byteBuffer);
  }

  private CompletableFuture<CacheGetResponse> sendGet(String cacheName, ByteString key) {
    checkCacheNameValid(cacheName);
    Optional<Span> span = buildSpan("java-sdk-get-request");
    Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));
    // Submit request to non-blocking stub
    ListenableFuture<_GetResponse> rspFuture =
        withCacheNameHeader(scsDataGrpcStubsManager.getStub(), cacheName).get(buildGetRequest(key));

    // Build a CompletableFuture to return to caller
    CompletableFuture<CacheGetResponse> returnFuture =
        new CompletableFuture<CacheGetResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_GetResponse>() {
          @Override
          public void onSuccess(_GetResponse rsp) {
            final ECacheResult result = rsp.getResult();

            final CacheGetResponse response;
            if (result == ECacheResult.Hit) {
              response = new CacheGetResponse.Hit(rsp.getCacheBody());
            } else if (result == ECacheResult.Miss) {
              response = new CacheGetResponse.Miss();
            } else {
              response =
                  new CacheGetResponse.Error(
                      new InternalServerException("Unsupported cache result " + result));
            }
            returnFuture.complete(response);
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          @Override
          public void onFailure(Throwable e) {
            returnFuture.complete(
                new CacheGetResponse.Error(CacheServiceExceptionMapper.convert(e)));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.ERROR);
                  theSpan.recordException(e);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheDeleteResponse> sendDelete(String cacheName, ByteString key) {
    checkCacheNameValid(cacheName);
    Optional<Span> span = buildSpan("java-sdk-delete-request");
    Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));
    // Submit request to non-blocking stub
    ListenableFuture<_DeleteResponse> rspFuture =
        withCacheNameHeader(scsDataGrpcStubsManager.getStub(), cacheName)
            .delete(buildDeleteRequest(key));

    // Build a CompletableFuture to return to caller
    CompletableFuture<CacheDeleteResponse> returnFuture =
        new CompletableFuture<CacheDeleteResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DeleteResponse>() {
          @Override
          public void onSuccess(_DeleteResponse rsp) {
            returnFuture.complete(new CacheDeleteResponse());
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          @Override
          public void onFailure(Throwable e) {
            returnFuture.completeExceptionally(CacheServiceExceptionMapper.convert(e));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.ERROR);
                  theSpan.recordException(e);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheSetResponse> sendSet(
      String cacheName, ByteString key, ByteString value, long ttlSeconds) {
    checkCacheNameValid(cacheName);
    Optional<Span> span = buildSpan("java-sdk-set-request");
    Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    ListenableFuture<_SetResponse> rspFuture =
        withCacheNameHeader(scsDataGrpcStubsManager.getStub(), cacheName)
            .set(buildSetRequest(key, value, ttlSeconds * 1000));

    // Build a CompletableFuture to return to caller
    CompletableFuture<CacheSetResponse> returnFuture =
        new CompletableFuture<CacheSetResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SetResponse>() {
          @Override
          public void onSuccess(_SetResponse rsp) {
            returnFuture.complete(new CacheSetResponse(value));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          @Override
          public void onFailure(Throwable e) {
            returnFuture.completeExceptionally(
                CacheServiceExceptionMapper.convert(e)); // bubble all errors up
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.ERROR);
                  theSpan.recordException(e);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private static ScsGrpc.ScsFutureStub withCacheNameHeader(
      ScsGrpc.ScsFutureStub stub, String cacheName) {
    Metadata header = new Metadata();
    header.put(CACHE_NAME_KEY, cacheName);
    return MetadataUtils.attachHeaders(stub, header);
  }

  private _GetRequest buildGetRequest(ByteString key) {
    return _GetRequest.newBuilder().setCacheKey(key).build();
  }

  private _DeleteRequest buildDeleteRequest(ByteString key) {
    return _DeleteRequest.newBuilder().setCacheKey(key).build();
  }

  private _SetRequest buildSetRequest(ByteString key, ByteString value, long ttl) {
    return _SetRequest.newBuilder()
        .setCacheKey(key)
        .setCacheBody(value)
        .setTtlMilliseconds(ttl)
        .build();
  }

  private Optional<Span> buildSpan(String spanName) {
    // TODO - We should change this logic so can pass in parent span so returned span becomes a sub
    // span of a parent span.
    return tracer.map(
        t ->
            t.spanBuilder(spanName)
                .setSpanKind(SpanKind.CLIENT)
                .setStartTimestamp(now())
                .startSpan());
  }

  @Override
  public void close() {
    scsDataGrpcStubsManager.close();
  }
}
