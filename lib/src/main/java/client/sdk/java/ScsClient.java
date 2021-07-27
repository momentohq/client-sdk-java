package client.sdk.java;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import grpc.cache_client.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * ScsClient is a client used to interact with the momento Simple Caching Service (SCS).
 */
public class ScsClient {
    private final ScsGrpc.ScsBlockingStub blockingStub;
    private final ScsGrpc.ScsFutureStub futureStub;
    private final ManagedChannel channel;

    /**
     * Builds an instance of {@link ScsClient} used to interact w/ SCS with a default endpoint.
     *
     * @param authToken Token to authenticate with SCS
     */
    ScsClient(String authToken) {
        this(authToken, "alpha.cacheservice.com");
    }

    /**
     * Builds an instance of {@link ScsClient} used to interact w/ SCS
     *
     * @param authToken Token to authenticate with SCS
     * @param endpoint  SCS endpoint to make api calls to
     */
    ScsClient(String authToken, String endpoint) {
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder.forAddress(
                endpoint,
                443
        );

        channelBuilder.useTransportSecurity();
        channelBuilder.disableRetry();
        channelBuilder.intercept(new AuthInterceptor(authToken));
        ManagedChannel channel = channelBuilder.build();
        this.blockingStub = ScsGrpc.newBlockingStub(channel);
        this.futureStub = ScsGrpc.newFutureStub(channel);
        this.channel = channel;
    }

    /**
     * Returns a requested object from cache specified by passed key. This method is a blocking api call. Please use
     * getAsync if you need a {@link java.util.concurrent.CompletionStage<ClientGetResponse>} returned instead.
     *
     * @param key the key of item to fetch from cache
     * @return {@link ClientGetResponse} with the response object as a {@link java.nio.ByteBuffer}
     * @throws IOException if an error occurs opening input stream for response body.
     */
    public ClientGetResponse<ByteBuffer> get(String key) throws IOException {
        GetResponse rsp = blockingStub.get(buildGetRequest(key));
        ByteBuffer body = rsp.getCacheBody().asReadOnlyByteBuffer();
        return new ClientGetResponse<>(rsp.getResult(), body);
    }

    /**
     * Sets an object in cache by the passed key. This method is a blocking api call. Please use
     * setAsync if you need a {@link java.util.concurrent.CompletionStage<ClientSetResponse>} returned instead.
     *
     * @param key        the key of item to fetch from cache
     * @param value      {@link ByteBuffer} of the value to set in cache
     * @param ttlSeconds the time for your object to live in cache in seconds.
     * @return {@link ClientSetResponse} with the result of the set operation
     * @throws IOException if an error occurs opening ByteBuffer for request body.
     */
    public ClientSetResponse set(String key, ByteBuffer value, int ttlSeconds) throws IOException {
        SetResponse rsp = blockingStub.set(buildSetRequest(key, value, ttlSeconds * 1000));
        return new ClientSetResponse(rsp.getResult());
    }

    /**
     * Returns CompletableStage of getting an item from SCS by passed key. Allows user of this clients
     * to better control concurrency of outbound cache get requests.
     *
     * @param key the key of item to fetch from cache.
     * @return {@link CompletionStage<ClientGetResponse>} Returns a CompletableFuture as a CompletionStage
     * interface wrapping standard ClientResponse with response object as a {@link java.io.InputStream}.
     */
    public CompletionStage<ClientGetResponse<ByteBuffer>> getAsync(String key) {

        // Submit request to non blocking stub
        ListenableFuture<GetResponse> rspFuture = futureStub.get(buildGetRequest(key));

        // Build a CompletableFuture to return to caller
        CompletableFuture<ClientGetResponse<ByteBuffer>> returnFuture = new CompletableFuture<ClientGetResponse<ByteBuffer>>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                // propagate cancel to the listenable future if called on returned completable future
                boolean result = rspFuture.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };

        // Convert returned ListenableFuture to CompletableFuture
        Futures.addCallback(rspFuture, new FutureCallback<GetResponse>() {
            @Override
            public void onSuccess(GetResponse rsp) {
                ByteBuffer body = rsp.getCacheBody().asReadOnlyByteBuffer();
                returnFuture.complete(new ClientGetResponse<>(rsp.getResult(), body));
            }

            @Override
            public void onFailure(Throwable e) {
                returnFuture.completeExceptionally(e);  // bubble all errors up
            }
        }, MoreExecutors.directExecutor()); // Execute on same thread that called execute on CompletionStage returned

        return returnFuture;
    }

    /**
     * Returns CompletableStage of setting an item in SCS by passed key. Allows user of this clients
     * to better control concurrency of outbound cache set requests.
     *
     * @param key        the key of item to fetch from cache.
     * @param value      {@link ByteBuffer} of the value to set in cache
     * @param ttlSeconds the time for your object to live in cache in seconds.
     * @return @{@link CompletionStage<ClientSetResponse>} Returns a CompletableFuture as a CompletionStage
     * interface wrapping standard ClientSetResponse.
     * @throws IOException if an error occurs opening ByteBuffer for request body.
     */
    public CompletionStage<ClientSetResponse> setAsync(String key, ByteBuffer value, int ttlSeconds) throws IOException {

        // Submit request to non blocking stub
        ListenableFuture<SetResponse> rspFuture = futureStub.set(buildSetRequest(key, value, ttlSeconds * 1000));

        // Build a CompletableFuture to return to caller
        CompletableFuture<ClientSetResponse> returnFuture = new CompletableFuture<ClientSetResponse>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                // propagate cancel to the listenable future if called on returned completable future
                boolean result = rspFuture.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };

        // Convert returned ListenableFuture to CompletableFuture
        Futures.addCallback(rspFuture, new FutureCallback<SetResponse>() {
            @Override
            public void onSuccess(SetResponse rsp) {
                returnFuture.complete(new ClientSetResponse(rsp.getResult()));
            }

            @Override
            public void onFailure(Throwable e) {
                returnFuture.completeExceptionally(e);  // bubble all errors up
            }
        }, MoreExecutors.directExecutor()); // Execute on same thread that called execute on CompletionStage returned

        return returnFuture;
    }

    public void close() throws InterruptedException {
        this.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private GetRequest buildGetRequest(String key) {
        return GetRequest
                .newBuilder()
                .setCacheKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
                .build();
    }

    private SetRequest buildSetRequest(String key, ByteBuffer value, int ttl) throws IOException {
        return SetRequest
                .newBuilder()
                .setCacheKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
                .setCacheBody(ByteString.readFrom(new ByteArrayInputStream(value.array())))
                .setTtlMilliseconds(ttl)
                .build();
    }

}
