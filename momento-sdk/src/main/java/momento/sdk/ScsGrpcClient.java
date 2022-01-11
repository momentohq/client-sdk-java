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
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.ImplicitContextKeyed;
import io.opentelemetry.context.Scope;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.net.ssl.SSLException;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;

/** Grpc wrapper responsible for maintaining Stubs, Channels to the Scs Service backend */
final class ScsGrpcClient implements Closeable {

  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);

  private final ScsGrpc.ScsFutureStub futureStub;
  private final ManagedChannel channel;
  private final Optional<Tracer> tracer;

  ScsGrpcClient(String authToken, String endpoint) {
    this(authToken, endpoint, Optional.empty());
  }

  ScsGrpcClient(String authToken, String endpoint, Optional<OpenTelemetry> openTelemetry) {
    this(authToken, endpoint, openTelemetry, false);
  }

  ScsGrpcClient(
      String authToken,
      String endpoint,
      Optional<OpenTelemetry> openTelemetry,
      boolean insecureSsl) {
    this.channel = setupChannel(authToken, endpoint, openTelemetry, insecureSsl);
    ScsGrpc.ScsBlockingStub stub = ScsGrpc.newBlockingStub(channel);
    this.futureStub = ScsGrpc.newFutureStub(channel);
    this.tracer = openTelemetry.map(ot -> ot.getTracer("momento-java-scs-client", "1.0.0"));
  }

  private ManagedChannel setupChannel(
      String authToken,
      String endpoint,
      Optional<OpenTelemetry> openTelemetry,
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
    openTelemetry.ifPresent(
        theOpenTelemetry ->
            clientInterceptors.add(new OpenTelemetryClientInterceptor(theOpenTelemetry)));
    channelBuilder.intercept(clientInterceptors);
    ManagedChannel channel = channelBuilder.build();
    return channel;
  }

  CompletableFuture<CacheGetResponse> sendGet(String cacheName, ByteString key) {
    Optional<Span> span = buildSpan("java-sdk-get-request");
    Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));
    // Submit request to non-blocking stub
    ListenableFuture<GetResponse> rspFuture =
        withCacheNameHeader(futureStub, cacheName).get(buildGetRequest(key));

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

  CompletableFuture<CacheSetResponse> sendSet(
      String cacheName, ByteString key, ByteString value, int ttlSeconds) {

    Optional<Span> span = buildSpan("java-sdk-set-request");
    Optional<Scope> scope = (span.map(ImplicitContextKeyed::makeCurrent));

    // Submit request to non-blocking stub
    ListenableFuture<SetResponse> rspFuture =
        withCacheNameHeader(futureStub, cacheName)
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
            returnFuture.complete(new CacheSetResponse(rsp.getResult()));
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

  @Override
  public void close() {
    channel.shutdown();
  }
}
