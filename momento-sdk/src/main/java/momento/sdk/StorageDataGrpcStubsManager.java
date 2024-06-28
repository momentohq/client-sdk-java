package momento.sdk;

import grpc.store.StoreGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfiguration;
import momento.sdk.exceptions.ConnectionFailedException;
import momento.sdk.internal.GrpcChannelOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager responsible for GRPC channels and stubs for the Data Plane.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class StorageDataGrpcStubsManager implements AutoCloseable {

  private final List<ManagedChannel> channels;
  private final List<StoreGrpc.StoreFutureStub> futureStubs;
  private final AtomicInteger nextStubIndex = new AtomicInteger(0);

  private final int numGrpcChannels;
  private final Duration deadline;
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageDataGrpcStubsManager.class);

  StorageDataGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider, @Nonnull StorageConfiguration configuration) {
    this.deadline = configuration.getTransportStrategy().getGrpcConfiguration().getDeadline();
    this.numGrpcChannels =
        configuration.getTransportStrategy().getGrpcConfiguration().getMinNumGrpcChannels();

    this.channels =
        IntStream.range(0, this.numGrpcChannels)
            .mapToObj(i -> setupChannel(credentialProvider, configuration))
            .collect(Collectors.toList());
    this.futureStubs = channels.stream().map(StoreGrpc::newFutureStub).collect(Collectors.toList());
  }

  /**
   * This method tries to connect to Momento's gRPC server during client initialization (eagerly).
   *
   * @param timeoutSeconds the timeout value beyond which to give up on the eager connection
   *     attempt.
   */
  public void connect(final long timeoutSeconds) {
    // TODO: client initialization time could be optimized, in the case where a user configures more
    // than one gRPC
    //  channel, by attempting to connect these channels asynchronously rather than serially.
    for (ManagedChannel channel : channels) {
      final ConnectivityState currentState = channel.getState(true /* tryToConnect */);
      if (ConnectivityState.READY.equals(currentState)) {
        LOGGER.debug("Connected to Momento's server! Happy Caching!");
        continue;
      }

      final CompletableFuture<Void> connectionFuture = new CompletableFuture<>();
      eagerlyConnect(
          currentState, connectionFuture, channel, Instant.now().plusSeconds(timeoutSeconds));

      try {
        connectionFuture.get(timeoutSeconds, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        connectionFuture.cancel(true);
        throw new ConnectionFailedException(
            "Failed to connect within the allotted time of " + timeoutSeconds + " seconds.", e);
      } catch (InterruptedException | ExecutionException e) {
        connectionFuture.cancel(true);
        throw new ConnectionFailedException(
            "Error while waiting for eager connection to establish.", e);
      }
    }
  }

  private static void eagerlyConnect(
      final ConnectivityState lastObservedState,
      final CompletableFuture<Void> connectionFuture,
      final ManagedChannel channel,
      final Instant timeout) {

    if (Instant.now().toEpochMilli() > timeout.toEpochMilli()) {
      connectionFuture.completeExceptionally(
          new ConnectionFailedException("Connection failed: Deadline exceeded"));
      return;
    }
    channel.notifyWhenStateChanged(
        lastObservedState,
        () -> {
          final ConnectivityState currentState = channel.getState(false /* tryToConnect */);
          switch (currentState) {
            case READY:
              LOGGER.debug("Connected to Momento's server! Happy Caching!");
              connectionFuture.complete(null);
              return;
            case IDLE:
              LOGGER.debug("State is idle; waiting to transition to CONNECTING");
              eagerlyConnect(currentState, connectionFuture, channel, timeout);
              break;
            case CONNECTING:
              LOGGER.debug("State transitioned to CONNECTING; waiting to get READY");
              eagerlyConnect(currentState, connectionFuture, channel, timeout);
              break;
            default:
              LOGGER.debug(
                  "Unexpected state encountered {}. Contact Momento if this persists.",
                  currentState.name());
              connectionFuture.completeExceptionally(
                  new ConnectionFailedException(
                      "Connection failed due to unexpected state: " + currentState));
              break;
          }
        });
  }

  private ManagedChannel setupChannel(
      CredentialProvider credentialProvider, StorageConfiguration configuration) {
    final NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(credentialProvider.getCacheEndpoint(), 443);

    // set additional channel options (message size, keepalive, auth, etc)
    GrpcChannelOptions.applyGrpcConfigurationToChannelBuilder(
        configuration.getTransportStrategy().getGrpcConfiguration(), channelBuilder);

    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new UserHeaderInterceptor(credentialProvider.getAuthToken(), "store"));
    channelBuilder.intercept(clientInterceptors);

    return channelBuilder.build();
  }

  /**
   * Returns a stub with appropriate deadlines.
   *
   * <p>Each stub is deliberately decorated with Deadline. Deadlines work differently than timeouts.
   * When a deadline is set on a stub, it simply means that once the stub is created it must be used
   * before the deadline expires. Hence, the stub returned from here should never be cached and the
   * safest behavior is for clients to request a new stub each time.
   *
   * <p><a href="https://github.com/grpc/grpc-java/issues/1495">more information</a>
   */
  StoreGrpc.StoreFutureStub getStub() {
    int nextStubIndex = this.nextStubIndex.getAndIncrement();
    return futureStubs
        .get(nextStubIndex % this.numGrpcChannels)
        .withDeadlineAfter(deadline.getSeconds(), TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    for (ManagedChannel channel : channels) {
      channel.shutdown();
    }
  }
}
