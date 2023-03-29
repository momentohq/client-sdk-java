package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static java.time.Instant.now;
import static momento.sdk.ValidationUtils.checkCacheNameValid;
import static momento.sdk.ValidationUtils.checkListNameValid;
import static momento.sdk.ValidationUtils.checkListSliceStartEndValid;
import static momento.sdk.ValidationUtils.checkSetNameValid;
import static momento.sdk.ValidationUtils.ensureValidCacheSet;
import static momento.sdk.ValidationUtils.ensureValidKey;
import static momento.sdk.ValidationUtils.ensureValidValue;

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
import grpc.cache_client._ListConcatenateBackRequest;
import grpc.cache_client._ListConcatenateBackResponse;
import grpc.cache_client._ListConcatenateFrontRequest;
import grpc.cache_client._ListConcatenateFrontResponse;
import grpc.cache_client._ListFetchRequest;
import grpc.cache_client._ListFetchResponse;
import grpc.cache_client._SetDifferenceRequest;
import grpc.cache_client._SetDifferenceResponse;
import grpc.cache_client._SetFetchRequest;
import grpc.cache_client._SetFetchResponse;
import grpc.cache_client._SetIfNotExistsRequest;
import grpc.cache_client._SetIfNotExistsResponse;
import grpc.cache_client._SetRequest;
import grpc.cache_client._SetResponse;
import grpc.cache_client._SetUnionRequest;
import grpc.cache_client._SetUnionResponse;
import grpc.cache_client._Unbounded;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.messages.CacheDeleteResponse;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheIncrementResponse;
import momento.sdk.messages.CacheListConcatenateBackResponse;
import momento.sdk.messages.CacheListConcatenateFrontResponse;
import momento.sdk.messages.CacheListFetchResponse;
import momento.sdk.messages.CacheSetAddElementResponse;
import momento.sdk.messages.CacheSetAddElementsResponse;
import momento.sdk.messages.CacheSetFetchResponse;
import momento.sdk.messages.CacheSetIfNotExistsResponse;
import momento.sdk.messages.CacheSetRemoveElementResponse;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.requests.CollectionTtl;

/** Client for interacting with Scs Data plane. */
final class ScsDataClient implements Closeable {

  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);

  private final Tracer tracer;
  private final Duration itemDefaultTtl;
  private final ScsDataGrpcStubsManager scsDataGrpcStubsManager;
  private final String endpoint;

  ScsDataClient(
      @Nonnull String authToken,
      @Nonnull String endpoint,
      @Nonnull Duration defaultTtl,
      @Nullable OpenTelemetry openTelemetry,
      @Nullable Duration requestTimeout) {
    if (openTelemetry != null) {
      this.tracer = openTelemetry.getTracer("momento-java-scs-client", "1.0.0");
    } else {
      this.tracer = null;
    }
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

  CompletableFuture<CacheSetAddElementResponse> setAddElement(
      String cacheName, String setName, String element, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElement(cacheName, convert(setName), convert(element), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetAddElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetAddElementResponse> setAddElement(
      String cacheName, String setName, byte[] element, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElement(cacheName, convert(setName), convert(element), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetAddElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetAddElementsResponse> setAddStringElements(
      String cacheName, String setName, Set<String> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(setName);
      ensureValidValue(elements);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElements(cacheName, convert(setName), convertStringSet(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetAddElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetAddElementsResponse> setAddByteArrayElements(
      String cacheName, String setName, Set<byte[]> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(setName);
      ensureValidValue(elements);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElements(cacheName, convert(setName), convertByteArraySet(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetAddElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetRemoveElementResponse> setRemoveElement(
      String cacheName, String setName, String element) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      return sendSetRemoveElement(cacheName, convert(setName), convert(element));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetRemoveElementResponse> setRemoveElement(
      String cacheName, String setName, byte[] element) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      return sendSetRemoveElement(cacheName, convert(setName), convert(element));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetFetchResponse> setFetch(String cacheName, String setName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(setName);
      return sendSetFetch(cacheName, convert(setName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBack(
      String cacheName,
      String listName,
      List<String> values,
      CollectionTtl ttl,
      Integer truncateFrontToSize) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendListConcatenateBack(
          cacheName, convert(listName), convertStringList(values), ttl, truncateFrontToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBack(
      String cacheName,
      String listName,
      List<byte[]> values,
      CollectionTtl ttl,
      int truncateFrontToSize) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendListConcatenateBack(
          cacheName, convert(listName), convertByteArrayList(values), ttl, truncateFrontToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFront(
      String cacheName,
      String listName,
      List<String> values,
      CollectionTtl ttl,
      Integer truncateBackToSize) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendListConcatenateFront(
          cacheName, convert(listName), convertStringList(values), ttl, truncateBackToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFront(
      String cacheName,
      String listName,
      List<byte[]> values,
      CollectionTtl ttl,
      int truncateBackToSize) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendListConcatenateFront(
          cacheName, convert(listName), convertByteArrayList(values), ttl, truncateBackToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListFetchResponse> listFetch(
      String cacheName, String listName, Integer startIndex, Integer endIndex) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      checkListSliceStartEndValid(startIndex, endIndex);
      return sendListFetch(cacheName, convert(listName), startIndex, endIndex);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
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

  private Set<ByteString> convertStringSet(Set<String> strings) {
    return strings.stream().map(this::convert).collect(Collectors.toSet());
  }

  private Set<ByteString> convertByteArraySet(Set<byte[]> strings) {
    return strings.stream().map(this::convert).collect(Collectors.toSet());
  }

  private List<ByteString> convertStringList(List<String> strings) {
    return strings.stream().map(this::convert).collect(Collectors.toList());
  }

  private List<ByteString> convertByteArrayList(List<byte[]> byteArrays) {
    return byteArrays.stream().map(this::convert).collect(Collectors.toList());
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
          public void onFailure(@Nonnull Throwable e) {
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
          public void onFailure(@Nonnull Throwable e) {
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
          public void onFailure(@Nonnull Throwable e) {
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
          public void onFailure(@Nonnull Throwable e) {
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

  private CompletableFuture<CacheSetAddElementResponse> sendSetAddElement(
      String cacheName, ByteString setName, ByteString element, CollectionTtl ttl) {
    final Optional<Span> span = buildSpan("java-sdk-set-add-element-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetUnionResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setUnion(buildSetUnionRequest(setName, Collections.singleton(element), ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetAddElementResponse> returnFuture =
        new CompletableFuture<CacheSetAddElementResponse>() {
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
        new FutureCallback<_SetUnionResponse>() {
          @Override
          public void onSuccess(_SetUnionResponse rsp) {
            returnFuture.complete(new CacheSetAddElementResponse.Success());
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetAddElementResponse.Error(
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
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSetAddElementsResponse> sendSetAddElements(
      String cacheName, ByteString setName, Set<ByteString> elements, CollectionTtl ttl) {
    final Optional<Span> span = buildSpan("java-sdk-set-add-elements-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetUnionResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setUnion(buildSetUnionRequest(setName, elements, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetAddElementsResponse> returnFuture =
        new CompletableFuture<CacheSetAddElementsResponse>() {
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
        new FutureCallback<_SetUnionResponse>() {
          @Override
          public void onSuccess(_SetUnionResponse rsp) {
            returnFuture.complete(new CacheSetAddElementsResponse.Success());
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetAddElementsResponse.Error(
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
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSetRemoveElementResponse> sendSetRemoveElement(
      String cacheName, ByteString setName, ByteString element) {
    final Optional<Span> span = buildSpan("java-sdk-set-remove-element-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetDifferenceResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setDifference(buildSetDifferenceRequest(setName, Collections.singleton(element)));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetRemoveElementResponse> returnFuture =
        new CompletableFuture<CacheSetRemoveElementResponse>() {
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
        new FutureCallback<_SetDifferenceResponse>() {
          @Override
          public void onSuccess(_SetDifferenceResponse rsp) {
            returnFuture.complete(new CacheSetRemoveElementResponse.Success());
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetRemoveElementResponse.Error(
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
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSetFetchResponse> sendSetFetch(
      String cacheName, ByteString setName) {
    checkCacheNameValid(cacheName);
    final Optional<Span> span = buildSpan("java-sdk-set-fetch-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setFetch(buildSetFetchRequest(setName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetFetchResponse> returnFuture =
        new CompletableFuture<CacheSetFetchResponse>() {
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
        new FutureCallback<_SetFetchResponse>() {
          @Override
          public void onSuccess(_SetFetchResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(
                  new CacheSetFetchResponse.Hit(rsp.getFound().getElementsList()));
            } else {
              returnFuture.complete(new CacheSetFetchResponse.Miss());
            }
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
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

  private CompletableFuture<CacheListConcatenateBackResponse> sendListConcatenateBack(
      String cacheName,
      ByteString listName,
      List<ByteString> values,
      CollectionTtl ttl,
      Integer truncateFrontToSize) {
    final Optional<Span> span = buildSpan("java-sdk-listConcatenateBack-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListConcatenateBackResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listConcatenateBack(
                buildListConcatenateBackRequest(listName, values, ttl, truncateFrontToSize));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListConcatenateBackResponse> returnFuture =
        new CompletableFuture<CacheListConcatenateBackResponse>() {
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
        new FutureCallback<_ListConcatenateBackResponse>() {
          @Override
          public void onSuccess(_ListConcatenateBackResponse rsp) {
            returnFuture.complete(
                new CacheListConcatenateBackResponse.Success(rsp.getListLength()));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListConcatenateBackResponse.Error(
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

  private CompletableFuture<CacheListConcatenateFrontResponse> sendListConcatenateFront(
      String cacheName,
      ByteString listName,
      List<ByteString> values,
      CollectionTtl ttl,
      Integer truncateBackToSize) {
    final Optional<Span> span = buildSpan("java-sdk-listConcatenateFront-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListConcatenateFrontResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listConcatenateFront(
                buildListConcatenateFrontRequest(listName, values, ttl, truncateBackToSize));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListConcatenateFrontResponse> returnFuture =
        new CompletableFuture<CacheListConcatenateFrontResponse>() {
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
        new FutureCallback<_ListConcatenateFrontResponse>() {
          @Override
          public void onSuccess(_ListConcatenateFrontResponse rsp) {
            returnFuture.complete(
                new CacheListConcatenateFrontResponse.Success(rsp.getListLength()));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListConcatenateFrontResponse.Error(
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

  private CompletableFuture<CacheListFetchResponse> sendListFetch(
      String cacheName, ByteString listName, Integer startIndex, Integer endIndex) {
    final Optional<Span> span = buildSpan("java-sdk-listFetch-request");
    final Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listFetch(buildListFetchRequest(listName, startIndex, endIndex));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListFetchResponse> returnFuture =
        new CompletableFuture<CacheListFetchResponse>() {
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
        new FutureCallback<_ListFetchResponse>() {
          @Override
          public void onSuccess(_ListFetchResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(new CacheListFetchResponse.Hit(rsp.getFound().getValuesList()));
              span.ifPresent(
                  theSpan -> {
                    theSpan.setStatus(StatusCode.OK);
                    theSpan.end(now());
                  });
              scope.ifPresent(Scope::close);
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new CacheListFetchResponse.Miss());
              span.ifPresent(
                  theSpan -> {
                    theSpan.setStatus(StatusCode.OK);
                    theSpan.end(now());
                  });
              scope.ifPresent(Scope::close);
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListFetchResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
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

  private _SetUnionRequest buildSetUnionRequest(
      ByteString setName, Set<ByteString> elements, CollectionTtl ttl) {
    return _SetUnionRequest.newBuilder()
        .setSetName(setName)
        .addAllElements(elements)
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private _SetDifferenceRequest buildSetDifferenceRequest(
      ByteString setName, Set<ByteString> elements) {
    return _SetDifferenceRequest.newBuilder()
        .setSetName(setName)
        .setSubtrahend(
            _SetDifferenceRequest._Subtrahend.newBuilder()
                .setSet(
                    _SetDifferenceRequest._Subtrahend._Set.newBuilder()
                        .addAllElements(elements)
                        .build())
                .build())
        .build();
  }

  private _SetFetchRequest buildSetFetchRequest(ByteString setName) {
    return _SetFetchRequest.newBuilder().setSetName(setName).build();
  }

  private _ListConcatenateBackRequest buildListConcatenateBackRequest(
      ByteString listName,
      List<ByteString> values,
      CollectionTtl ttl,
      Integer truncateFrontToSize) {
    _ListConcatenateBackRequest request =
        _ListConcatenateBackRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .setTruncateFrontToSize(truncateFrontToSize.byteValue())
            .addAllValues(values)
            .build();
    return request;
  }

  private _ListConcatenateFrontRequest buildListConcatenateFrontRequest(
      ByteString listName, List<ByteString> values, CollectionTtl ttl, Integer truncateBackToSize) {
    _ListConcatenateFrontRequest request =
        _ListConcatenateFrontRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .setTruncateBackToSize(truncateBackToSize.byteValue())
            .addAllValues(values)
            .build();
    return request;
  }

  private _ListFetchRequest buildListFetchRequest(
      ByteString listName, Integer startIndex, Integer endIndex) {
    _ListFetchRequest request;
    if (startIndex != null && endIndex != null) {
      request =
          _ListFetchRequest.newBuilder()
              .setListName(listName)
              .setInclusiveStart(startIndex)
              .setExclusiveEnd(endIndex)
              .build();
    } else if (startIndex != null && endIndex == null) {
      request =
          _ListFetchRequest.newBuilder()
              .setListName(listName)
              .setInclusiveStart(startIndex)
              .setUnboundedEnd(_Unbounded.newBuilder().build())
              .build();
    } else if (startIndex == null && endIndex != null) {
      request =
          _ListFetchRequest.newBuilder()
              .setListName(listName)
              .setUnboundedStart(_Unbounded.newBuilder().build())
              .setExclusiveEnd(endIndex)
              .build();
    } else {
      request =
          _ListFetchRequest.newBuilder()
              .setListName(listName)
              .setUnboundedStart(_Unbounded.newBuilder().build())
              .setUnboundedEnd(_Unbounded.newBuilder().build())
              .build();
    }

    return request;
  }

  private Optional<Span> buildSpan(String spanName) {
    // TODO - We should change this logic so can pass in parent span so returned span becomes a sub
    // span of a parent span.
    return Optional.ofNullable(tracer)
        .map(
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
