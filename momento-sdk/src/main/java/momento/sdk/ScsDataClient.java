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
import grpc.cache_client.ECacheResult;
import grpc.cache_client.ScsGrpc;
import grpc.cache_client._DeleteRequest;
import grpc.cache_client._DeleteResponse;
import grpc.cache_client._GetRequest;
import grpc.cache_client._GetResponse;
import grpc.cache_client._IncrementRequest;
import grpc.cache_client._IncrementResponse;
import grpc.cache_client._SetIfNotExistsRequest;
import grpc.cache_client._SetIfNotExistsResponse;
import grpc.cache_client._SetRequest;
import grpc.cache_client._SetResponse;
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
import momento.sdk.messages.CacheIncrementResponse;
import momento.sdk.messages.CacheSetIfNotExistsResponse;
import momento.sdk.messages.CacheSetResponse;

/** Client for interacting with Scs Data plane. */
final class ScsDataClient implements Closeable {

  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);

  private final Optional<Tracer> tracer;
  private final Duration itemDefaultTtl;
  private final ScsDataGrpcStubsManager scsDataGrpcStubsManager;
  private final String endpoint;

  ScsDataClient(
      String authToken,
      String endpoint,
      Duration defaultTtl,
      Optional<OpenTelemetry> openTelemetry,
      Optional<Duration> requestTimeout) {
    this.tracer = openTelemetry.map(ot -> ot.getTracer("momento-java-scs-client", "1.0.0"));
    this.itemDefaultTtl = defaultTtl;
    this.scsDataGrpcStubsManager =
        new ScsDataGrpcStubsManager(authToken, endpoint, openTelemetry, requestTimeout);
    this.endpoint = endpoint;
  }

  public String getEndpoint() {
    return endpoint;
  }

  CompletableFuture<CacheGetResponse> get(String cacheName, byte[] key) {
    try {
      ensureValidKey(key);
      return sendGet(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheGetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheGetResponse> get(String cacheName, String key) {
    try {
      ensureValidKey(key);
      return sendGet(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheGetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDeleteResponse> delete(String cacheName, byte[] key) {
    try {
      ensureValidKey(key);
      return sendDelete(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDeleteResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDeleteResponse> delete(String cacheName, String key) {
    try {
      ensureValidKey(key);
      return sendDelete(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDeleteResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, ByteBuffer value, Duration ttl) {
    try {
      ensureValidCacheSet(key, value, ttl);
      return sendSet(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetResponse> set(String cacheName, String key, ByteBuffer value) {
    return set(cacheName, key, value, itemDefaultTtl);
  }

  CompletableFuture<CacheSetResponse> set(
      String cacheName, byte[] key, byte[] value, Duration ttl) {
    try {
      ensureValidCacheSet(key, value, ttl);
      return sendSet(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetResponse> set(String cacheName, byte[] key, byte[] value) {
    return set(cacheName, key, value, itemDefaultTtl);
  }

  CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, String value, Duration ttl) {
    try {
      ensureValidCacheSet(key, value, ttl);
      return sendSet(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, String field, long amount, Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      return sendIncrement(cacheName, convert(field), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheIncrementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, byte[] field, long amount, Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      return sendIncrement(cacheName, convert(field), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheIncrementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetResponse> set(String cacheName, String key, String value) {
    return set(cacheName, key, value, itemDefaultTtl);
  }

  CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, String value, Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, byte[] value, Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, String value, Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, byte[] value, Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
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

  private CompletableFuture<CacheGetResponse> sendGet(String cacheName, ByteString key) {
    checkCacheNameValid(cacheName);
    final Optional<Span> span = buildSpan("java-sdk-get-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_GetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata).get(buildGetRequest(key));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheGetResponse> returnFuture =
        new CompletableFuture<CacheGetResponse>() {
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
                      new InternalServerException("Unsupported cache Get result: " + result));
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
                new CacheGetResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.ERROR);
                  theSpan.recordException(e);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheDeleteResponse> sendDelete(String cacheName, ByteString key) {
    checkCacheNameValid(cacheName);
    final Optional<Span> span = buildSpan("java-sdk-delete-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));
    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DeleteResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata).delete(buildDeleteRequest(key));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDeleteResponse> returnFuture =
        new CompletableFuture<CacheDeleteResponse>() {
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
        new FutureCallback<_DeleteResponse>() {
          @Override
          public void onSuccess(_DeleteResponse rsp) {
            returnFuture.complete(new CacheDeleteResponse.Success());
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
                new CacheDeleteResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.ERROR);
                  theSpan.recordException(e);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSetResponse> sendSet(
      String cacheName, ByteString key, ByteString value, Duration ttl) {
    checkCacheNameValid(cacheName);
    final Optional<Span> span = buildSpan("java-sdk-set-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .set(buildSetRequest(key, value, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetResponse> returnFuture =
        new CompletableFuture<CacheSetResponse>() {
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
        new FutureCallback<_SetResponse>() {
          @Override
          public void onSuccess(_SetResponse rsp) {
            returnFuture.complete(new CacheSetResponse.Success(value));
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
                new CacheSetResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.ERROR);
                  theSpan.recordException(e);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheIncrementResponse> sendIncrement(
      String cacheName, ByteString field, long amount, Duration ttl) {
    final Optional<Span> span = buildSpan("java-sdk-increment-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_IncrementResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .increment(buildIncrementRequest(field, amount, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheIncrementResponse> returnFuture =
        new CompletableFuture<CacheIncrementResponse>() {
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
        new FutureCallback<_IncrementResponse>() {
          @Override
          public void onSuccess(_IncrementResponse rsp) {
            returnFuture.complete(new CacheIncrementResponse.Success((int) rsp.getValue()));
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
                new CacheIncrementResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
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

  private CompletableFuture<CacheSetIfNotExistsResponse> sendSetIfNotExists(
      String cacheName, ByteString key, ByteString value, Duration ttl) {
    final Optional<Span> span = buildSpan("java-sdk-setIfNotExists-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetIfNotExistsResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setIfNotExists(buildSetIfNotExistsRequest(key, value, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetIfNotExistsResponse> returnFuture =
        new CompletableFuture<CacheSetIfNotExistsResponse>() {
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
        new FutureCallback<_SetIfNotExistsResponse>() {
          @Override
          public void onSuccess(_SetIfNotExistsResponse rsp) {
            if (rsp.getResultCase().equals(_SetIfNotExistsResponse.ResultCase.STORED)) {
              returnFuture.complete(new CacheSetIfNotExistsResponse.Stored(key, value));
              span.ifPresent(
                  theSpan -> {
                    theSpan.setStatus(StatusCode.OK);
                    theSpan.end(now());
                  });
              scope.ifPresent(Scope::close);
            } else if (rsp.getResultCase().equals(_SetIfNotExistsResponse.ResultCase.NOT_STORED)) {
              returnFuture.complete(new CacheSetIfNotExistsResponse.NotStored());
              span.ifPresent(
                  theSpan -> {
                    theSpan.setStatus(StatusCode.OK);
                    theSpan.end(now());
                  });
              scope.ifPresent(Scope::close);
            }
          }

          @Override
          public void onFailure(Throwable e) {
            returnFuture.complete(
                new CacheSetIfNotExistsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
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

  private static Metadata metadataWithCache(String cacheName) {
    final Metadata metadata = new Metadata();
    metadata.put(CACHE_NAME_KEY, cacheName);

    return metadata;
  }

  private static ScsGrpc.ScsFutureStub attachMetadata(
      ScsGrpc.ScsFutureStub stub, Metadata metadata) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  private _GetRequest buildGetRequest(ByteString key) {
    return _GetRequest.newBuilder().setCacheKey(key).build();
  }

  private _DeleteRequest buildDeleteRequest(ByteString key) {
    return _DeleteRequest.newBuilder().setCacheKey(key).build();
  }

  private _SetRequest buildSetRequest(ByteString key, ByteString value, Duration ttl) {
    return _SetRequest.newBuilder()
        .setCacheKey(key)
        .setCacheBody(value)
        .setTtlMilliseconds(ttl.toMillis())
        .build();
  }

  private _IncrementRequest buildIncrementRequest(ByteString field, long amount, Duration ttl) {
    return _IncrementRequest.newBuilder()
        .setCacheKey(field)
        .setAmount(amount)
        .setTtlMilliseconds(ttl.toMillis())
        .build();
  }

  private _SetIfNotExistsRequest buildSetIfNotExistsRequest(
      ByteString key, ByteString value, Duration ttl) {
    return _SetIfNotExistsRequest.newBuilder()
        .setCacheKey(key)
        .setCacheBody(value)
        .setTtlMilliseconds(ttl.toMillis())
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
