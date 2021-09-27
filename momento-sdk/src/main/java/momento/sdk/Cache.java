package momento.sdk;

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
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import momento.sdk.messages.ClientGetResponse;
import momento.sdk.messages.ClientSetResponse;

/** Client to perform operations on cache. */
// TODO: https://github.com/momentohq/client-sdk-java/issues/24 - constructors should be visible
// only in the package.
// TODO: Also clean up the method and class comments
public class Cache {
  private final ScsGrpc.ScsBlockingStub blockingStub;
  private final ScsGrpc.ScsFutureStub futureStub;
  private final ManagedChannel channel;
  private final Optional<Tracer> tracer;

  /**
   * Builds an instance of {@link Cache} used to interact w/ SCS with a default endpoint.
   *
   * @param authToken Token to authenticate with Cache Service
   */
  public Cache(String authToken, String cacheName) {
    this(authToken, cacheName, Optional.empty());
  }

  /**
   * Builds an instance of {@link Cache} that will interact with a specified endpoint
   *
   * @param authToken Token to authenticate with SCS
   * @param endpoint SCS endpoint to make api calls to
   */
  public Cache(String authToken, String cacheName, String endpoint) {
    this(authToken, cacheName, Optional.empty(), endpoint);
  }

  /**
   * @param authToken Token to authenticate with SCS
   * @param openTelemetry Open telemetry instance to hook into client traces
   */
  public Cache(String authToken, String cacheName, Optional<OpenTelemetry> openTelemetry) {
    this(authToken, cacheName, openTelemetry, "alpha.cacheservice.com");
  }

  /**
   * Builds an instance of {@link Cache} used to interact w/ SCS
   *
   * @param authToken Token to authenticate with SCS
   * @param openTelemetry Open telemetry instance to hook into client traces
   * @param endpoint SCS endpoint to make api calls to
   */
  public Cache(
      String authToken, String cacheName, Optional<OpenTelemetry> openTelemetry, String endpoint) {
    this(authToken, cacheName, openTelemetry, endpoint, false);
  }

  /**
   * Builds an instance of {@link Cache} used to interact w/ SCS
   *
   * @param authToken Token to authenticate with SCS
   * @param openTelemetry Open telemetry instance to hook into client traces
   * @param endpoint SCS endpoint to make api calls to
   * @param insecureSsl for overriding host validation
   */
  public Cache(
      String authToken,
      String cacheName,
      Optional<OpenTelemetry> openTelemetry,
      String endpoint,
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
    this.tracer =
        openTelemetry
            .map(
                theOpenTelemetry ->
                    Optional.of(theOpenTelemetry.getTracer("momento-java-scs-client", "1.0.0")))
            .orElse(Optional.empty());
  }

  /**
   * Returns a requested object from cache specified by passed key. This method is a blocking api
   * call. Please use getAsync if you need a {@link java.util.concurrent.CompletionStage<
   * ClientGetResponse >} returned instead.
   *
   * @param key the key of item to fetch from cache
   * @return {@link ClientGetResponse} with the response object as a {@link java.nio.ByteBuffer}
   * @throws IOException if an error occurs opening input stream for response body.
   */
  public ClientGetResponse<ByteBuffer> get(String key) throws IOException {
    String spanName = "java-sdk-get-request";
    // TODO - We should change this logic so that the span becomes a sub span of a parent span.
    Optional<Span> span =
        tracer
            .map(
                theTracer ->
                    Optional.of(
                        theTracer
                            .spanBuilder(spanName)
                            .setSpanKind(SpanKind.CLIENT)
                            .setStartTimestamp(Instant.now())
                            .startSpan()))
            .orElse(Optional.empty());
    try (Scope scope = (span.isPresent() ? span.get().makeCurrent() : null)) {
      GetResponse rsp = blockingStub.get(buildGetRequest(key));
      ByteBuffer body = rsp.getCacheBody().asReadOnlyByteBuffer();
      ClientGetResponse clientGetResponse = new ClientGetResponse<>(rsp.getResult(), body);
      span.ifPresent(theSpan -> theSpan.setStatus(StatusCode.OK));
      return clientGetResponse;
    } catch (Exception e) {
      span.ifPresent(
          theSpan -> {
            theSpan.recordException(e);
            theSpan.setStatus(StatusCode.ERROR);
          });
      // TODO - Yup I know this is ugly but we have work to do to handle excpetions properly.
      throw e;
    } finally {
      span.ifPresent(theSpan -> theSpan.end(Instant.now()));
    }
  }

  /**
   * Sets an object in cache by the passed key. This method is a blocking api call. Please use
   * setAsync if you need a {@link java.util.concurrent.CompletionStage< ClientSetResponse >}
   * returned instead.
   *
   * @param key the key of item to fetch from cache
   * @param value {@link ByteBuffer} of the value to set in cache
   * @param ttlSeconds the time for your object to live in cache in seconds.
   * @return {@link ClientSetResponse} with the result of the set operation
   * @throws IOException if an error occurs opening ByteBuffer for request body.
   */
  public ClientSetResponse set(String key, ByteBuffer value, int ttlSeconds) throws IOException {
    String spanName = "java-sdk-set-request";
    // TODO - We should change this logic so that the span becomes a sub span of a parent span.
    Optional<Span> span =
        tracer
            .map(
                theTracer ->
                    Optional.of(
                        theTracer
                            .spanBuilder(spanName)
                            .setSpanKind(SpanKind.CLIENT)
                            .setStartTimestamp(Instant.now())
                            .startSpan()))
            .orElse(Optional.empty());
    try (Scope scope = (span.isPresent() ? span.get().makeCurrent() : null)) {
      SetResponse rsp = blockingStub.set(buildSetRequest(key, value, ttlSeconds * 1000));
      ClientSetResponse response = new ClientSetResponse(rsp.getResult());
      span.ifPresent(theSpan -> theSpan.setStatus(StatusCode.OK));
      return response;
    } catch (Exception e) {
      span.ifPresent(
          theSpan -> {
            theSpan.recordException(e);
            theSpan.setStatus(StatusCode.ERROR);
          });
      // TODO - Yup I know this is ugly but we have work to do to handle exceptions.
      throw e;
    } finally {
      span.ifPresent(theSpan -> theSpan.end(Instant.now()));
    }
  }

  /**
   * Returns CompletableStage of getting an item from SCS by passed key. Allows user of this clients
   * to better control concurrency of outbound cache get requests.
   *
   * @param key the key of item to fetch from cache.
   * @return {@link CompletionStage<ClientGetResponse>} Returns a CompletableFuture as a
   *     CompletionStage interface wrapping standard ClientResponse with response object as a {@link
   *     java.io.InputStream}.
   */
  public CompletionStage<ClientGetResponse<ByteBuffer>> getAsync(String key) {
    String spanName = "java-sdk-get-request";
    // TODO - We should change this logic so that the span becomes a sub span of a parent span.
    Optional<Span> span =
        tracer
            .map(
                theTracer ->
                    Optional.of(
                        theTracer
                            .spanBuilder(spanName)
                            .setSpanKind(SpanKind.CLIENT)
                            .setStartTimestamp(Instant.now())
                            .startSpan()))
            .orElse(Optional.empty());
    Optional<Scope> scope =
        (span.isPresent() ? Optional.of(span.get().makeCurrent()) : Optional.empty());
    // Submit request to non blocking stub
    ListenableFuture<GetResponse> rspFuture = futureStub.get(buildGetRequest(key));

    // Build a CompletableFuture to return to caller
    CompletableFuture<ClientGetResponse<ByteBuffer>> returnFuture =
        new CompletableFuture<ClientGetResponse<ByteBuffer>>() {
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
            ByteBuffer body = rsp.getCacheBody().asReadOnlyByteBuffer();
            returnFuture.complete(new ClientGetResponse<>(rsp.getResult(), body));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(Instant.now());
                });
            scope.ifPresent(theScope -> theScope.close());
          }

          @Override
          public void onFailure(Throwable e) {
            returnFuture.completeExceptionally(e);
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.ERROR);
                  theSpan.recordException(e);
                  theSpan.end(Instant.now());
                });
            scope.ifPresent(theScope -> theScope.close());
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  /**
   * Returns CompletableStage of setting an item in SCS by passed key. Allows user of this clients
   * to better control concurrency of outbound cache set requests.
   *
   * @param key the key of item to fetch from cache.
   * @param value {@link ByteBuffer} of the value to set in cache
   * @param ttlSeconds the time for your object to live in cache in seconds.
   * @return @{@link CompletionStage<ClientSetResponse>} Returns a CompletableFuture as a
   *     CompletionStage interface wrapping standard ClientSetResponse.
   * @throws IOException if an error occurs opening ByteBuffer for request body.
   */
  public CompletionStage<ClientSetResponse> setAsync(String key, ByteBuffer value, int ttlSeconds)
      throws IOException {
    String spanName = "java-sdk-set-request";
    // TODO - We should change this logic so that the span becomes a sub span of a parent span.
    Optional<Span> span =
        tracer
            .map(
                theTracer ->
                    Optional.of(
                        theTracer
                            .spanBuilder(spanName)
                            .setSpanKind(SpanKind.CLIENT)
                            .setStartTimestamp(Instant.now())
                            .startSpan()))
            .orElse(Optional.empty());
    Optional<Scope> scope =
        (span.isPresent() ? Optional.of(span.get().makeCurrent()) : Optional.empty());
    // Submit request to non blocking stub
    ListenableFuture<SetResponse> rspFuture =
        futureStub.set(buildSetRequest(key, value, ttlSeconds * 1000));

    // Build a CompletableFuture to return to caller
    CompletableFuture<ClientSetResponse> returnFuture =
        new CompletableFuture<ClientSetResponse>() {
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
            returnFuture.complete(new ClientSetResponse(rsp.getResult()));
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.OK);
                  theSpan.end(Instant.now());
                });
            scope.ifPresent(theScope -> theScope.close());
          }

          @Override
          public void onFailure(Throwable e) {
            returnFuture.completeExceptionally(e); // bubble all errors up
            span.ifPresent(
                theSpan -> {
                  theSpan.setStatus(StatusCode.ERROR);
                  theSpan.recordException(e);
                  theSpan.end(Instant.now());
                });
            scope.ifPresent(theScope -> theScope.close());
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  public void close() throws InterruptedException {
    this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  private GetRequest buildGetRequest(String key) {
    return GetRequest.newBuilder()
        .setCacheKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
        .build();
  }

  private SetRequest buildSetRequest(String key, ByteBuffer value, int ttl) throws IOException {
    return SetRequest.newBuilder()
        .setCacheKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
        .setCacheBody(ByteString.readFrom(new ByteArrayInputStream(value.array())))
        .setTtlMilliseconds(ttl)
        .build();
  }
}
