package momento.sdk;

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
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.ImplicitContextKeyed;
import io.opentelemetry.context.Scope;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.SSLException;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;

/** Client to perform operations on cache. */
public final class Cache implements Closeable {
  private final ScsGrpc.ScsBlockingStub blockingStub;
  private final ScsGrpc.ScsFutureStub futureStub;
  private final ManagedChannel channel;
  private final Optional<Tracer> tracer;
  private final int itemDefaultTtlSeconds;

  Cache(String authToken, String cacheName, String endpoint, int itemDefaultTtlSeconds) {
    this(authToken, cacheName, Optional.empty(), endpoint, itemDefaultTtlSeconds);
  }

  Cache(
      String authToken,
      String cacheName,
      Optional<OpenTelemetry> openTelemetry,
      String endpoint,
      int itemDefaultTtlSeconds) {
    this(authToken, cacheName, openTelemetry, endpoint, itemDefaultTtlSeconds, false);
  }

  Cache(
      String authToken,
      String cacheName,
      Optional<OpenTelemetry> openTelemetry,
      String endpoint,
      int itemDefaultTtlSeconds,
      boolean insecureSsl) {
    NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(endpoint, 443);

    if (insecureSsl) {
      try {
        channelBuilder.sslContext(
            GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build());
      } catch (SSLException e) {
        throw new RuntimeException("Unable to use insecure trust manager", e);
      }
    }
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();
    List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new AuthInterceptor(authToken));
    clientInterceptors.add(new CacheNameInterceptor(cacheName));
    openTelemetry.ifPresent(
        theOpenTelemetry ->
            clientInterceptors.add(new OpenTelemetryClientInterceptor(theOpenTelemetry)));
    channelBuilder.intercept(clientInterceptors);
    ManagedChannel channel = channelBuilder.build();
    this.blockingStub = ScsGrpc.newBlockingStub(channel);
    this.futureStub = ScsGrpc.newFutureStub(channel);
    this.channel = channel;
    this.tracer = openTelemetry.map(ot -> ot.getTracer("momento-java-scs-client", "1.0.0"));
    this.itemDefaultTtlSeconds = itemDefaultTtlSeconds;
    waitTillReady();
  }

  // TODO: Temporary measure for beta. This will not be required soon.
  private void waitTillReady() {
    long start = System.currentTimeMillis();
    long maxRetryDurationMillis = 5000;
    long backoffDurationMillis = 5;
    StatusRuntimeException lastRetriedException = null;

    while (System.currentTimeMillis() - start < maxRetryDurationMillis) {
      try {
        // The key has no special meaning. Just any key string would work.
        this.blockingStub.get(buildGetRequest(convert("000")));
        return;
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.UNKNOWN
            || e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
          try {
            Thread.sleep(backoffDurationMillis);
          } catch (InterruptedException t) {
            throw CacheServiceExceptionMapper.convert(t);
          }
          lastRetriedException = e;
        } else {
          throw CacheServiceExceptionMapper.convert(e);
        }
      }
    }
    throw CacheServiceExceptionMapper.convertUnhandledExceptions(lastRetriedException);
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param key The key to get
   * @return {@link CacheGetResponse} containing the status of the get operation and the associated
   *     value data.
   * @see Cache#getAsync(String)
   */
  public CacheGetResponse get(String key) {
    ensureValidKey(key);
    return sendGet(convert(key));
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param key The key to get
   * @return {@link CacheGetResponse} containing the status of the get operation and the associated
   *     value data.
   * @see Cache#getAsync(byte[])
   */
  public CacheGetResponse get(byte[] key) {
    ensureValidKey(key);
    return sendGet(convert(key));
  }

  private CacheGetResponse sendGet(ByteString key) {
    Optional<Span> span = buildSpan("java-sdk-get-request");
    try (Scope ignored = (span.map(ImplicitContextKeyed::makeCurrent).orElse(null))) {
      GetResponse rsp = blockingStub.get(buildGetRequest(key));
      CacheGetResponse cacheGetResponse = new CacheGetResponse(rsp.getResult(), rsp.getCacheBody());
      span.ifPresent(theSpan -> theSpan.setStatus(StatusCode.OK));
      return cacheGetResponse;
    } catch (Exception e) {
      span.ifPresent(
          theSpan -> {
            theSpan.recordException(e);
            theSpan.setStatus(StatusCode.ERROR);
          });
      throw CacheServiceExceptionMapper.convert(e);
    } finally {
      span.ifPresent(theSpan -> theSpan.end(now()));
    }
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Result of the set operation.
   * @see Cache#set(String, ByteBuffer)
   * @see Cache#setAsync(String, ByteBuffer, int)
   */
  public CacheSetResponse set(String key, ByteBuffer value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendSet(convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Result of the set operation.
   * @see Cache#set(String, ByteBuffer, int)
   * @see Cache#setAsync(String, ByteBuffer)
   */
  public CacheSetResponse set(String key, ByteBuffer value) {
    return set(key, value, itemDefaultTtlSeconds);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Result of the set operation.
   * @see Cache#set(String, String)
   * @see Cache#setAsync(String, String, int)
   */
  public CacheSetResponse set(String key, String value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendSet(convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Result of the set operation.
   * @see Cache#set(String, String, int)
   * @see Cache#setAsync(String, String)
   */
  public CacheSetResponse set(String key, String value) {
    return set(key, value, itemDefaultTtlSeconds);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Result of the set operation.
   * @see Cache#set(byte[], byte[])
   * @see Cache#setAsync(byte[], byte[], int)
   */
  public CacheSetResponse set(byte[] key, byte[] value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendSet(convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Result of the set operation.
   * @see Cache#set(byte[], byte[], int)
   * @see Cache#setAsync(byte[], byte[])
   */
  public CacheSetResponse set(byte[] key, byte[] value) {
    return set(key, value, itemDefaultTtlSeconds);
  }

  // Having this method named as set causes client side compilation issues, where the compiler
  // requires a dependency
  // on com.google.protobuf.ByteString
  private CacheSetResponse sendSet(ByteString key, ByteString value, int ttlSeconds) {
    Optional<Span> span = buildSpan("java-sdk-set-request");
    try (Scope ignored = (span.map(ImplicitContextKeyed::makeCurrent).orElse(null))) {
      SetResponse rsp = blockingStub.set(buildSetRequest(key, value, ttlSeconds * 1000));

      CacheSetResponse response = new CacheSetResponse(rsp.getResult());
      span.ifPresent(theSpan -> theSpan.setStatus(StatusCode.OK));
      return response;
    } catch (Exception e) {
      span.ifPresent(
          theSpan -> {
            theSpan.recordException(e);
            theSpan.setStatus(StatusCode.ERROR);
          });
      throw CacheServiceExceptionMapper.convert(e);
    } finally {
      span.ifPresent(theSpan -> theSpan.end(now()));
    }
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param key The key to get
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   * @see Cache#get(byte[])
   */
  public CompletableFuture<CacheGetResponse> getAsync(byte[] key) {
    ensureValidKey(key);
    return sendAsyncGet(convert(key));
  }

  /**
   * Get the cache value stored for the given key.
   *
   * @param key The key to get
   * @return Future with {@link CacheGetResponse} containing the status of the get operation and the
   *     associated value data.
   * @see Cache#get(String)
   */
  public CompletableFuture<CacheGetResponse> getAsync(String key) {
    ensureValidKey(key);
    return sendAsyncGet(convert(key));
  }

  private CompletableFuture<CacheGetResponse> sendAsyncGet(ByteString key) {
    Optional<Span> span = buildSpan("java-sdk-get-request");
    Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));
    // Submit request to non-blocking stub
    ListenableFuture<GetResponse> rspFuture = futureStub.get(buildGetRequest(key));

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

          // TODO: Handle Exception Mapping
          @Override
          public void onFailure(Throwable e) {
            returnFuture.completeExceptionally(e);
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

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Future containing the result of the set operation.
   * @see Cache#setAsync(String, ByteBuffer)
   * @see Cache#set(String, ByteBuffer, int)
   */
  public CompletableFuture<CacheSetResponse> setAsync(
      String key, ByteBuffer value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendSetAsync(convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   * @see Cache#set(String, ByteBuffer)
   * @see Cache#setAsync(String, ByteBuffer, int)
   */
  public CompletableFuture<CacheSetResponse> setAsync(String key, ByteBuffer value) {
    return setAsync(key, value, itemDefaultTtlSeconds);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Future containing the result of the set operation.
   * @see Cache#setAsync(byte[], byte[])
   * @see Cache#set(byte[], byte[], int)
   */
  public CompletableFuture<CacheSetResponse> setAsync(byte[] key, byte[] value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendSetAsync(convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   * @see Cache#setAsync(byte[], byte[], int)
   * @see Cache#set(byte[], byte[])
   */
  public CompletableFuture<CacheSetResponse> setAsync(byte[] key, byte[] value) {
    return setAsync(key, value, itemDefaultTtlSeconds);
  }

  /**
   * Sets the value in cache with a given Time To Live (TTL) seconds.
   *
   * <p>If a value for this key is already present it will be replaced by the new value.
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @param ttlSeconds Time to Live for the item in Cache. This ttl takes precedence over the TTL
   *     used when building a cache client {@link Momento#cacheBuilder(String, int)}
   * @return Future containing the result of the set operation.
   * @see Cache#setAsync(String, String)
   * @see Cache#set(String, String, int)
   */
  public CompletableFuture<CacheSetResponse> setAsync(String key, String value, int ttlSeconds) {
    ensureValid(key, value, ttlSeconds);
    return sendSetAsync(convert(key), convert(value), ttlSeconds);
  }

  /**
   * Sets the value in the cache. If a value for this key is already present it will be replaced by
   * the new value.
   *
   * <p>The Time to Live (TTL) seconds defaults to the parameter used when building this Cache
   * client - {@link Momento#cacheBuilder(String, int)}
   *
   * @param key The key under which the value is to be added.
   * @param value The value to be stored.
   * @return Future containing the result of the set operation.
   * @see Cache#setAsync(String, String, int)
   * @see Cache#set(String, String)
   */
  public CompletableFuture<CacheSetResponse> setAsync(String key, String value) {
    return setAsync(key, value, itemDefaultTtlSeconds);
  }

  private CompletableFuture<CacheSetResponse> sendSetAsync(
      ByteString key, ByteString value, int ttlSeconds) {

    Optional<Span> span = buildSpan("java-sdk-set-request");
    Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    ListenableFuture<SetResponse> rspFuture =
        futureStub.set(buildSetRequest(key, value, ttlSeconds * 1000));

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
            returnFuture.complete(new CacheSetResponse(rsp.getResult()));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(now());
                });
            scope.ifPresent(Scope::close);
          }

          // TODO: Handle Exception Mapping
          @Override
          public void onFailure(Throwable e) {
            returnFuture.completeExceptionally(e); // bubble all errors up
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

  /** Shutdown the client. */
  public void close() {
    this.channel.shutdown();
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

  private static void ensureValid(Object key, Object value, int ttlSeconds) {

    ensureValidKey(key);

    if (value == null) {
      throw new ClientSdkException("A non-null value is required.");
    }

    if (ttlSeconds <= 0) {
      throw new ClientSdkException("Item's time to live in Cache must be a positive integer.");
    }
  }

  private static void ensureValidKey(Object key) {
    if (key == null) {
      throw new ClientSdkException("A non-null Key is required.");
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
}
