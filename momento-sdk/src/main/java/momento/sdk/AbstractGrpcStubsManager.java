package momento.sdk;

import io.grpc.ClientInterceptor;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.config.ReadConcern;
import momento.sdk.config.transport.IGrpcConfiguration;
import momento.sdk.exceptions.ConnectionFailedException;
import momento.sdk.internal.GrpcChannelOptions;
import momento.sdk.retry.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractGrpcStubsManager<S extends AbstractStub<S>> implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGrpcStubsManager.class);

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
  private final RetryStrategy retryStrategy;

  private static final int MAX_RETRY_THREAD_POOL_SIZE = 64;
  // Timeout to keep threads idle/alive in the retry thread pool
  private static final long RETRY_THREAD_POOL_KEEP_ALIVE_SECONDS = 60L;

  protected final List<ManagedChannel> channels;
  protected final List<S> futureStubs;
  protected final AtomicInteger nextStubIndex = new AtomicInteger(0);
  @Nullable protected final Duration deadline;
  protected final int numGrpcChannels;

  protected AbstractGrpcStubsManager(@Nonnull Config<S> config) {
    this.deadline = config.grpcConfiguration.getDeadline();
    this.numGrpcChannels = config.grpcConfiguration.getMinNumGrpcChannels();
    this.channels = new ArrayList<>(numGrpcChannels);
    this.futureStubs = new ArrayList<>(numGrpcChannels);

    if (config.retryStrategy != null) {
      this.retryStrategy = config.retryStrategy;
      this.retryScheduler = Executors.newSingleThreadScheduledExecutor();
      this.retryExecutor =
          new ThreadPoolExecutor(
              0,
              MAX_RETRY_THREAD_POOL_SIZE,
              RETRY_THREAD_POOL_KEEP_ALIVE_SECONDS,
              TimeUnit.SECONDS,
              new LinkedBlockingQueue<>());
    } else {
      this.retryStrategy = null;
      this.retryScheduler = null;
      this.retryExecutor = null;
    }

    channels.addAll(
        IntStream.range(0, this.numGrpcChannels)
            .mapToObj(
                i ->
                    setupConnection(
                        config.clientType,
                        config.endpoint,
                        config.authToken,
                        config.grpcConfiguration,
                        config.readConcern))
            .collect(Collectors.toList()));
    futureStubs.addAll(channels.stream().map(config.stubFactory).collect(Collectors.toList()));
  }

  private ManagedChannel setupConnection(
      String clientType,
      String endpoint,
      String authToken,
      IGrpcConfiguration grpcConfiguration,
      @Nullable ReadConcern readConcern) {
    final NettyChannelBuilder channelBuilder = NettyChannelBuilder.forAddress(endpoint, 443);

    // set additional channel options (message size, keepalive, auth, etc)
    GrpcChannelOptions.applyGrpcConfigurationToChannelBuilder(grpcConfiguration, channelBuilder);

    final Map<Metadata.Key<String>, String> extraHeaders = new HashMap<>();
    if (readConcern != null && readConcern != ReadConcern.BALANCED) {
      extraHeaders.put(UserHeaderInterceptor.READ_CONCERN, readConcern.toLowerCase());
    }

    final List<ClientInterceptor> clientInterceptors = new ArrayList<>();
    clientInterceptors.add(new UserHeaderInterceptor(authToken, clientType, extraHeaders));

    if (retryStrategy != null) {
      clientInterceptors.add(
          new RetryClientInterceptor(retryStrategy, retryScheduler, retryExecutor));
    }

    channelBuilder.intercept(clientInterceptors);
    return channelBuilder.build();
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

  protected <T extends AbstractStub<T>> T getNextStub(List<T> stubs) {
    int nextStubIndex = this.nextStubIndex.getAndIncrement();
    T stub = stubs.get(nextStubIndex % this.numGrpcChannels);
    if (deadline != null) {
      stub = stub.withDeadlineAfter(deadline.toMillis(), TimeUnit.MILLISECONDS);
    }
    return stub;
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
  S getStub() {
    return getNextStub(futureStubs);
  }

  @Override
  public void close() {
    doClose();
    boolean wasInterrupted = false;

    // Initiate shutdown of all channels and then wait for them to shut down
    for (ManagedChannel channel : channels) {
      channel.shutdown();
    }
    for (ManagedChannel channel : channels) {
      try {
        if (!channel.awaitTermination(20, TimeUnit.SECONDS)) {
          channel.shutdownNow();
          if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
            LOGGER.warn("Channel failed to shut down within 30 seconds");
          }
        }
      } catch (InterruptedException e) {
        channel.shutdownNow();
        wasInterrupted = true;
      }
    }

    // Shut down retry executors if they are present. They are not likely be processing anything
    // since the channels are shut down.
    if (retryExecutor != null) {
      retryExecutor.shutdown();
      try {
        if (!retryExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          retryExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        retryExecutor.shutdownNow();
        wasInterrupted = true;
      }
    }
    if (retryScheduler != null) {
      retryScheduler.shutdown();
      try {
        if (!retryScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
          retryScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        retryScheduler.shutdownNow();
        wasInterrupted = true;
      }
    }

    if (wasInterrupted) {
      Thread.currentThread().interrupt();
    }
  }

  protected void doClose() {}

  protected static class Config<S extends AbstractStub<S>> {
    @Nonnull public final String clientType;
    @Nonnull public final String endpoint;
    @Nonnull public final String authToken;
    @Nonnull public final IGrpcConfiguration grpcConfiguration;
    @Nonnull public final Function<ManagedChannel, S> stubFactory;
    @Nullable public final RetryStrategy retryStrategy;
    @Nullable public final ReadConcern readConcern;

    private Config(
        @Nonnull String clientType,
        @Nonnull String endpoint,
        @Nonnull String authToken,
        @Nonnull IGrpcConfiguration grpcConfiguration,
        @Nonnull Function<ManagedChannel, S> stubFactory,
        @Nullable RetryStrategy retryStrategy,
        @Nullable ReadConcern readConcern) {
      this.clientType = clientType;
      this.endpoint = endpoint;
      this.authToken = authToken;
      this.grpcConfiguration = grpcConfiguration;
      this.stubFactory = stubFactory;
      this.retryStrategy = retryStrategy;
      this.readConcern = readConcern;
    }

    public static <S extends AbstractStub<S>> Config<S> create(
        @Nonnull String clientType,
        @Nonnull String endpoint,
        @Nonnull String authToken,
        @Nonnull IGrpcConfiguration grpcConfiguration,
        @Nonnull Function<ManagedChannel, S> stubFactory) {
      return new Config<>(
          clientType, endpoint, authToken, grpcConfiguration, stubFactory, null, null);
    }

    public Config<S> withRetryStrategy(RetryStrategy retryStrategy) {
      return new Config<>(
          clientType,
          endpoint,
          authToken,
          grpcConfiguration,
          stubFactory,
          retryStrategy,
          readConcern);
    }

    public Config<S> withReadConcern(ReadConcern readConcern) {
      return new Config<>(
          clientType,
          endpoint,
          authToken,
          grpcConfiguration,
          stubFactory,
          retryStrategy,
          readConcern);
    }
  }
}
