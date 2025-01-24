package momento.sdk;

import grpc.cache_client.ScsGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.ReadConcern;
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
final class ScsDataGrpcStubsManager implements AutoCloseable {

  private final List<ManagedChannel> channels;
  private final List<ScsGrpc.ScsFutureStub> futureStubs;
  private final List<ScsGrpc.ScsStub> observableStubs;
  private final AtomicInteger nextStubIndex = new AtomicInteger(0);

  private final int numGrpcChannels;
  private final Duration deadline;

  /**
   * These two executors are used by {@link RetryClientInterceptor} to schedule and execute retries
   * on failed gRPC requests. One is a single threaded scheduling executor {@link
   * ScheduledExecutorService} that's only responsible to schedule retries with a given delay. The
   * other executor {@link ThreadPoolExecutor} is a dynamic executor that spawns threads as
   * necessary and executes the retries (essentially doing the I/O work).
   *
   * <p>{@link ScheduledExecutorService} creates idle threads and hence the choice of using two
   * executors. The small overhead of a single thread doing the scheduling negates the implications
   * of creating potentially hundreds of idle threads with the ScheduledExecutorService. This
   * executor is chosen to avoid a Thread.sleep() while doing retries. There are no available
   * configurations to ScheduledExecutorService such that it grows dynamically.
   *
   * <p>The {@link ThreadPoolExecutor} has a size of 0 and grows onDemand. The maximumPoolSize is
   * present to cap the number of threads we create for retries; where the {@link
   * LinkedBlockingQueue} essentially stores any pending retries. Note that the keep alive time for
   * each thread is 60 seconds, beyond which the JVM will destroy the thread. This is the default
   * value of {@link Executors#newCachedThreadPool()} and also applies in our situation as even with
   * backoffs, one retry should be below 60 seconds. We aren't directly using a cached thread pool
   * because it grows unbounded.
   */
  private final ScheduledExecutorService retryScheduler;

  private final ExecutorService retryExecutor;

  // An arbitrary selection of twice the number of available processors.
  private static final int MAX_RETRY_THREAD_POOL_SIZE = 64;
  // Timeout to keep threads idle/alive in the retry thread pool
  private static final long RETRY_THREAD_POOL_KEEP_ALIVE_SECONDS = 60L;

  private static final Logger LOGGER = LoggerFactory.getLogger(ScsDataGrpcStubsManager.class);

  ScsDataGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider, @Nonnull Configuration configuration) {
    this.deadline = configuration.getTransportStrategy().getGrpcConfiguration().getDeadline();
    this.numGrpcChannels =
        configuration.getTransportStrategy().getGrpcConfiguration().getMinNumGrpcChannels();

    this.retryScheduler = Executors.newSingleThreadScheduledExecutor();
    this.retryExecutor =
        new ThreadPoolExecutor(
            0,
            MAX_RETRY_THREAD_POOL_SIZE,
            RETRY_THREAD_POOL_KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>());

    this.channels =
        IntStream.range(0, this.numGrpcChannels)
            .mapToObj(i -> setupChannel(credentialProvider, configuration))
            .collect(Collectors.toList());
    this.futureStubs = channels.stream().map(ScsGrpc::newFutureStub).collect(Collectors.toList());
    this.observableStubs = channels.stream().map(ScsGrpc::newStub).collect(Collectors.toList());
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
      CredentialProvider credentialProvider, Configuration configuration) {
    int port = credentialProvider.getPort();
    final NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(credentialProvider.getCacheEndpoint(), port);

    // set additional channel options (message size, keepalive, auth, etc)
    GrpcChannelOptions.applyGrpcConfigurationToChannelBuilder(
        configuration.getTransportStrategy().getGrpcConfiguration(),
        channelBuilder,
        credentialProvider.isEndpointSecure(credentialProvider.getCacheEndpoint()));

    final Map<Metadata.Key<String>, String> extraHeaders = new HashMap<>();
    if (configuration.getReadConcern() != ReadConcern.BALANCED) {
      extraHeaders.put(
          UserHeaderInterceptor.READ_CONCERN, configuration.getReadConcern().toLowerCase());
    }

    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(
        new UserHeaderInterceptor(credentialProvider.getAuthToken(), "cache", extraHeaders));
    clientInterceptors.add(
        new RetryClientInterceptor(
            configuration.getRetryStrategy(), retryScheduler, retryExecutor));
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
  ScsGrpc.ScsFutureStub getStub() {
    int nextStubIndex = this.nextStubIndex.getAndIncrement();
    return futureStubs
        .get(nextStubIndex % this.numGrpcChannels)
        .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Returns a stream observable stub with appropriate deadlines.
   *
   * <p>Each stub is deliberately decorated with Deadline. Deadlines work differently than timeouts.
   * When a deadline is set on a stub, it simply means that once the stub is created it must be used
   * before the deadline expires. Hence, the stub returned from here should never be cached and the
   * safest behavior is for clients to request a new stub each time.
   *
   * <p><a href="https://github.com/grpc/grpc-java/issues/1495">more information</a>
   */
  ScsGrpc.ScsStub getObservableStub() {
    int nextStubIndex = this.nextStubIndex.getAndIncrement();
    return observableStubs
        .get(nextStubIndex % this.numGrpcChannels)
        .withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    retryScheduler.shutdown();
    retryExecutor.shutdown();
    for (ManagedChannel channel : channels) {
      channel.shutdown();
    }
  }
}
