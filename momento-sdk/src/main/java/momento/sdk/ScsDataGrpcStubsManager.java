package momento.sdk;

import grpc.cache_client.ScsGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager responsible for GRPC channels and stubs for the Data Plane.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsDataGrpcStubsManager implements Closeable {

  private final ManagedChannel channel;
  private final ScsGrpc.ScsFutureStub futureStub;
  private final Duration deadline;
  private final ScheduledExecutorService retryScheduler;
  private final ExecutorService retryExecutor;

  // An arbitary selection of twice the number of available processors.
  private static final int MAX_RETRY_THREAD_POOL_SIZE = 64;
  // Timeout to keep threads idle/alive in the retry thread pool
  private static final long RETRY_THREAD_POOL_KEEP_ALIVE_SECONDS = 60L;

  private static final Logger LOGGER = LoggerFactory.getLogger(ScsDataGrpcStubsManager.class);

  ScsDataGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider, @Nonnull Configuration configuration) {
    this.deadline = configuration.getTransportStrategy().getGrpcConfiguration().getDeadline();

    /**
     * These two executors are used by {@link RetryClientInterceptor} to schedule and execute
     * retries on failed gRPC requests. One is a single threaded scheduling executor {@link
     * ScheduledExecutorService} that's only responsible to schedule retries with a given delay. The
     * other executor {@link ThreadPoolExecutor} is a dynamic executor that spawns threads as
     * necessary and executes the retries (essentially doing the I/O work).
     *
     * <p>{@link ScheduledExecutorService} creates idle threads and hence the choice of using two
     * executors. The small overhead of a single thread doing the scheduling negates the
     * implications of creating potentially hundreds of idle threads with the
     * ScheduledExecutorService. This executor is chosen to avoid a Thread.sleep() while doing
     * retries. There are no available configurations to ScheduledExecutorService such that it grows
     * dynamically.
     *
     * <p>The {@link ThreadPoolExecutor} has a size of 0 and grows onDemand. The maximumPoolSize is
     * present to cap the number of threads we create for retries; where the {@link
     * LinkedBlockingQueue} essentially stores any pending retries. Note that the keep alive time
     * for each thread is 60 seconds, beyond which the JVM will destroy the thread. This is the
     * default value of {@link Executors.newCachedThreadPool()} and also applies in our situation as
     * even with backoffs, one retry should be below 60 seconds. We aren't directly using a cached
     * thread pool because it grows unbounded.
     */
    this.retryScheduler = Executors.newSingleThreadScheduledExecutor();
    this.retryExecutor =
        new ThreadPoolExecutor(
            0,
            MAX_RETRY_THREAD_POOL_SIZE,
            RETRY_THREAD_POOL_KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>());

    this.channel = setupChannel(credentialProvider, configuration);
    this.futureStub = ScsGrpc.newFutureStub(channel);
  }

  /**
   * This method tries to connect to Momento's gRPC server during client initialization (eagerly).
   *
   * @param timeoutSeconds the timeout value beyond which to give up on the eager connection
   *     attempt.
   */
  public void connect(final long timeoutSeconds) {
    final ConnectivityState currentState = this.channel.getState(true /* tryToConnect */);
    if (ConnectivityState.READY.equals(currentState)) {
      LOGGER.debug("Connected to Momento's server! Happy Caching!");
      return;
    }

    // this future is our signalling mechanism to exit after the eager connection is successfully
    // established or fails
    final CompletableFuture<Void> connectionFuture = new CompletableFuture<>();
    eagerlyConnect(currentState, connectionFuture);

    try {
      connectionFuture.get(timeoutSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      connectionFuture.cancel(true);
      LOGGER.debug("Failed to connect within the allotted time of {} seconds.", 1);
    } catch (InterruptedException | ExecutionException e) {
      connectionFuture.cancel(true);
      LOGGER.debug("Error while waiting for eager connection to establish.", e);
    }
  }

  private void eagerlyConnect(
      final ConnectivityState lastObservedState, final CompletableFuture<Void> connectionFuture) {

    // the callback is triggerd only when a state change happens
    this.channel.notifyWhenStateChanged(
        lastObservedState,
        () -> {
          final ConnectivityState currentState = this.channel.getState(false /* tryToConnect */);
          switch (currentState) {
            case READY:
              LOGGER.debug("Connected to Momento's server! Happy Caching!");
              connectionFuture.complete(null);
              break;
            case IDLE:
              LOGGER.debug("State is idle; waiting to transition to CONNECTING");
              // we will recursively call this atmost twice (IDLE -> CONNECTING and CONNECTING ->
              // READY) in a happy case.
              // we deliberately give up connecting eagerly if it fails or has intermittent issues
              // to not
              // blow up the call stack on frequent state changes.
              eagerlyConnect(currentState, connectionFuture);
              break;
            case CONNECTING:
              LOGGER.debug("State transitioned to CONNECTING; waiting to get READY");
              eagerlyConnect(currentState, connectionFuture);
              break;
            default:
              LOGGER.debug(
                  "Unexpected state encountered {}. Contact Momento if this persists.",
                  currentState.name());
              connectionFuture.complete(null);
              break;
          }
        });
  }

  private ManagedChannel setupChannel(
      CredentialProvider credentialProvider, Configuration configuration) {
    final NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forAddress(credentialProvider.getCacheEndpoint(), 443);
    channelBuilder.useTransportSecurity();
    channelBuilder.disableRetry();
    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new UserHeaderInterceptor(credentialProvider.getAuthToken()));
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
    return futureStub.withDeadlineAfter(deadline.getSeconds(), TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    retryScheduler.shutdown();
    retryExecutor.shutdown();
    channel.shutdown();
  }
}
