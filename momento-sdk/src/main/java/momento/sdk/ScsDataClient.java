package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static java.time.Instant.now;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import grpc.cache_client.GetRequest;
import grpc.cache_client.GetResponse;
import grpc.cache_client.ScsGrpc;
import grpc.cache_client.SetRequest;
import grpc.cache_client.SetResponse;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.SdkException;
import momento.sdk.exceptions.ValidationException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;

/** Client for interacting with Scs Data plane. */
final class ScsDataClient implements Closeable {

  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);

  private final Optional<Tracer> tracer;
  private int itemDefaultTtlSeconds;
  private ScsDataGrpcStubsManager scsDataGrpcStubsManager;

  ScsDataClient(
      String authToken,
      String endpoint,
      int defaultTtlSeconds,
      Optional<OpenTelemetry> openTelemetry) {
    this.tracer = openTelemetry.map(ot -> ot.getTracer("momento-java-scs-client", "1.0.0"));
    this.itemDefaultTtlSeconds = defaultTtlSeconds;
    this.scsDataGrpcStubsManager = new ScsDataGrpcStubsManager(authToken, endpoint, openTelemetry);
  }

  CacheGetResponse get(String cacheName, String key) {
    ensureValidKey(key);
    return sendBlockingGet(cacheName, convert(key));
  }

  CacheGetResponse get(String cacheName, byte[] key) {
    ensureValidKey(key);
    return sendBlockingGet(cacheName, convert(key));
  }

  CacheSetResponse set(String cacheName, String key, ByteBuffer value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendBlockingSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  CacheSetResponse set(String cacheName, String key, ByteBuffer value) {
    return set(cacheName, key, value, itemDefaultTtlSeconds);
  }

  CacheSetResponse set(String cacheName, String key, String value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendBlockingSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  CacheSetResponse set(String cacheName, String key, String value) {
    return set(cacheName, key, value, itemDefaultTtlSeconds);
  }

  CacheSetResponse set(String cacheName, byte[] key, byte[] value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendBlockingSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  CacheSetResponse set(String cacheName, byte[] key, byte[] value) {
    return set(cacheName, key, value, itemDefaultTtlSeconds);
  }

  CompletableFuture<CacheGetResponse> getAsync(String cacheName, byte[] key) {
    ensureValidKey(key);
    return sendGet(cacheName, convert(key));
  }

  CompletableFuture<CacheGetResponse> getAsync(String cacheName, String key) {
    ensureValidKey(key);
    return sendGet(cacheName, convert(key));
  }

  CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, String key, ByteBuffer value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(String cacheName, String key, ByteBuffer value) {
    return setAsync(cacheName, key, value, itemDefaultTtlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, byte[] key, byte[] value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(String cacheName, byte[] key, byte[] value) {
    return setAsync(cacheName, key, value, itemDefaultTtlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(
      String cacheName, String key, String value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendSet(cacheName, convert(key), convert(value), ttlSeconds);
  }

  CompletableFuture<CacheSetResponse> setAsync(String cacheName, String key, String value) {
    return setAsync(cacheName, key, value, itemDefaultTtlSeconds);
  }

  private static void ensureValid(Object key, Object value, int ttlSeconds) {

    ensureValidKey(key);

    if (value == null) {
      throw new ValidationException("A non-null value is required.");
    }

    if (ttlSeconds < 0) {
      throw new ValidationException("Item's time to live in Cache cannot be negative.");
    }
  }

  private static void ensureValidKey(Object key) {
    if (key == null) {
      throw new ValidationException("A non-null Key is required.");
    }
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

  private static SdkException handleExceptionally(Throwable t) {
    if (t instanceof ExecutionException) {
      return CacheServiceExceptionMapper.convert(t.getCause());
    }
    return CacheServiceExceptionMapper.convert(t);
  }

  private CacheGetResponse sendBlockingGet(String cacheName, ByteString key) {
    try {
      return sendGet(cacheName, key).get();
    } catch (Throwable t) {
      throw handleExceptionally(t);
    }
  }

  private CacheSetResponse sendBlockingSet(
      String cacheName, ByteString key, ByteString value, int itemTtlSeconds) {
    try {
      return sendSet(cacheName, key, value, itemTtlSeconds).get();
    } catch (Throwable t) {
      throw handleExceptionally(t);
    }
  }

  private CompletableFuture<CacheGetResponse> sendGet(String cacheName, ByteString key) {
    checkCacheNameValid(cacheName);
    Optional<Span> span = buildSpan("java-sdk-get-request");
    Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));
    // Submit request to non-blocking stub
    ListenableFuture<GetResponse> rspFuture =
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
        new FutureCallback<GetResponse>() {
          @Override
          public void onSuccess(GetResponse rsp) {
            returnFuture.complete(new CacheGetResponse(rsp.getResult(), rsp.getCacheBody()));
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
      String cacheName, ByteString key, ByteString value, int ttlSeconds) {
    checkCacheNameValid(cacheName);
    Optional<Span> span = buildSpan("java-sdk-set-request");
    Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    ListenableFuture<SetResponse> rspFuture =
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
        new FutureCallback<SetResponse>() {
          @Override
          public void onSuccess(SetResponse rsp) {
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

  private GetRequest buildGetRequest(ByteString key) {
    return GetRequest.newBuilder().setCacheKey(key).build();
  }

  private SetRequest buildSetRequest(ByteString key, ByteString value, int ttl) {
    return SetRequest.newBuilder()
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

  private static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new ValidationException("Cache Name is required.");
    }
  }

  @Override
  public void close() {
    scsDataGrpcStubsManager.close();
  }
}
