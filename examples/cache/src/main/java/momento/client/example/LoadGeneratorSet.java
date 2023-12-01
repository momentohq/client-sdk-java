package momento.client.example;

import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.SetResponse;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
public class LoadGeneratorSet {
  private static final String API_KEY_ENV_VAR = "MOMENTO_API_KEY";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofHours(24);
  private static final String CACHE_NAME = "java-loadgen";
  private static final Logger logger = LoggerFactory.getLogger(LoadGenerator.class);

  private final ScheduledExecutorService executorService;
  private final RateLimiter rateLimiter;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

  private final int requestsPerSecond;

  private final ConcurrentHistogram setHistogram = new ConcurrentHistogram(3);
  private final ConcurrentHistogram getHistogram = new ConcurrentHistogram(3);
  private final LongAdder globalRequestCount = new LongAdder();
  private final LongAdder globalSuccessCount = new LongAdder();
  private final LongAdder globalUnavailableCount = new LongAdder();
  private final LongAdder globalTimeoutCount = new LongAdder();
  private final LongAdder globalLimitExceededCount = new LongAdder();

  private final String cacheValue;
  private final CacheClient client;

  private final long startTime;

  public LoadGeneratorSet(
      int statsInterval,
      int maxConcurrentRequests,
      int requestsPerSecond,
      int cacheValueLength,
      int warmupTime) {
    cacheValue = "x".repeat(cacheValueLength);

    final CredentialProvider credentialProvider;
    try {
      credentialProvider = new EnvVarCredentialProvider(API_KEY_ENV_VAR);
    } catch (SdkException e) {
      logger.error("Unable to load credential from environment variable " + API_KEY_ENV_VAR, e);
      throw e;
    }
    client = CacheClient.create(credentialProvider, Configurations.Laptop.v1(), DEFAULT_ITEM_TTL);

    client.createCache(CACHE_NAME);

    executorService = Executors.newScheduledThreadPool(maxConcurrentRequests);
    rateLimiter = RateLimiter.create(requestsPerSecond);
    this.requestsPerSecond = requestsPerSecond;

    startTime = System.currentTimeMillis();

    // Schedule initial tasks
    for (int i = 0; i < maxConcurrentRequests; ++i) {
      scheduleSet(i, 0);
    }

    // Schedule a histogram reset after the warmup
    scheduler.schedule(setHistogram::reset, warmupTime, TimeUnit.SECONDS);

    // Schedule a task to print the stats
    scheduler.scheduleAtFixedRate(this::logInfo, statsInterval, statsInterval, TimeUnit.SECONDS);
  }

  private void scheduleSet(int workerId, int operationNum) {
    scheduleOperation(
        workerId,
        operationNum,
        key -> client.set(CACHE_NAME, key, cacheValue),
        (response, operationNumValue) -> {
          if (response instanceof SetResponse.Success) {
            globalSuccessCount.increment();
          } else if (response instanceof SetResponse.Error error) {
            handleErrorResponse(error.getErrorCode());
          }
        },
        setHistogram);
  }

  private void scheduleGet(int workerId, int operationNum) {
    final int nextOperationNum = operationNum + 1;
    scheduleOperation(
        workerId,
        operationNum,
        key -> client.get(CACHE_NAME, key),
        (response, operationNumValue) -> {
          if (response instanceof GetResponse.Hit || response instanceof GetResponse.Miss) {
            globalSuccessCount.increment();
          } else if (response instanceof GetResponse.Error error) {
            handleErrorResponse(error.getErrorCode());
          }
          scheduleSet(workerId, nextOperationNum);
        },
        getHistogram);
  }

  private void scheduleDelete(int workerId, int operationNum) {
    final int nextOperationNum = operationNum + 1;
    scheduleOperation(
        workerId,
        operationNum,
        key -> client.delete(CACHE_NAME, key),
        (response, operationNumValue) -> {
          if (response instanceof DeleteResponse.Success) {
            globalSuccessCount.increment();
          } else if (response instanceof GetResponse.Error error) {
            handleErrorResponse(error.getErrorCode());
          }
          scheduleSet(workerId, nextOperationNum);
        },
        getHistogram);
  }

  private <T> void scheduleOperation(
      int workerId,
      int operationNum,
      Function<String, CompletableFuture<T>> operation,
      BiConsumer<T, Integer> responseHandler,
      ConcurrentHistogram histogram) {
    final String key = "worker" + workerId + "operation" + operationNum;
    executorService.schedule(
        () -> {
          rateLimiter.acquire();
          final long startTime = System.nanoTime();
          T response = operation.apply(key).join();
          final long endTime = System.nanoTime();

          globalRequestCount.increment();
          responseHandler.accept(response, operationNum);

          histogram.recordValue(endTime - startTime);
        },
        0,
        TimeUnit.MILLISECONDS);
  }

  private void handleErrorResponse(MomentoErrorCode errorCode) {
    switch (errorCode) {
      case TIMEOUT_ERROR -> globalTimeoutCount.increment();
      case LIMIT_EXCEEDED_ERROR -> globalLimitExceededCount.increment();
    }
  }

  private void logInfo() {
    final StringBuilder builder = new StringBuilder();
    builder.append("\nCumulative stats:\n");
    final long requestCount = globalRequestCount.sum();
    builder
        .append(
            String.format(
                "%18s: %d (%.2f) tps, limited to %d tps",
                "total requests", requestCount, formatTps(requestCount), requestsPerSecond))
        .append('\n');

    final long successCount = globalSuccessCount.sum();
    builder.append(formatStat("success", requestCount, successCount)).append('\n');

    final long unavailableCount = globalUnavailableCount.sum();
    builder.append(formatStat("server unavailable", requestCount, unavailableCount)).append('\n');

    final long timeoutCount = globalTimeoutCount.sum();
    builder.append(formatStat("timeout", requestCount, timeoutCount)).append('\n');

    final long limitExceededCount = globalLimitExceededCount.sum();
    builder.append(formatStat("limit exceeded", requestCount, limitExceededCount)).append('\n');

    builder.append("\nCumulative write latencies:\n");
    builder.append(formatHistogram(setHistogram));

    builder.append("\nCumulative read latencies:\n");
    builder.append(formatHistogram(getHistogram));

    logger.info(builder.toString());
  }

  private String formatStat(String name, long totalRequests, long requests) {
    final double requestPercentage;
    if (totalRequests == 0) {
      requestPercentage = 0.0;
    } else {
      requestPercentage = (double) requests / totalRequests * 100;
    }
    return String.format("%18s: %d (%.2f)", name, requests, requestPercentage);
  }

  private double formatTps(long totalRequests) {
    final long elapsedTime = System.currentTimeMillis() - startTime;
    return totalRequests * 1000.0 / elapsedTime;
  }

  private String formatHistogram(Histogram histogram) {
    return String.format("%5s: %d\n", "count", histogram.getTotalCount())
        + String.format("%5s: %.2f\n", "min", histogram.getMinValue() / 1_000_000.0)
        + String.format("%5s: %.2f\n", "p50", histogram.getValueAtPercentile(50.0) / 1_000_000.0)
        + String.format("%5s: %.2f\n", "p90", histogram.getValueAtPercentile(90.0) / 1_000_000.0)
        + String.format("%5s: %.2f\n", "p95", histogram.getValueAtPercentile(95.0) / 1_000_000.0)
        + String.format("%5s: %.2f\n", "p96", histogram.getValueAtPercentile(96.0) / 1_000_000.0)
        + String.format("%5s: %.2f\n", "p97", histogram.getValueAtPercentile(97.0) / 1_000_000.0)
        + String.format("%5s: %.2f\n", "p98", histogram.getValueAtPercentile(98.0) / 1_000_000.0)
        + String.format("%5s: %.2f\n", "p99", histogram.getValueAtPercentile(99.0) / 1_000_000.0)
        + String.format("%5s: %.2f\n", "p99.9", histogram.getValueAtPercentile(99.9) / 1_000_000.0)
        + String.format("%5s: %.2f\n", "max", histogram.getMaxValue() / 1_000_000.0);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void shutdown() throws InterruptedException {
    executorService.shutdown();
    scheduler.shutdown();
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    client.close();
  }

  public static void main(String[] args) throws InterruptedException {
    //
    // Each time this amount of time in seconds has passed, statistics about throughput and latency
    // will be printed.
    //
    final int showStatsInterval = 10;
    //
    // Controls the size of the payload that will be used for the cache items in
    // the load test. Smaller payloads will generally provide lower latencies than
    // larger payloads.
    //
    final int cacheItemPayloadBytes = 200;
    //
    // Controls the number of concurrent requests that will be made (via asynchronous
    // function calls) by the load test. Increasing this number may improve throughput,
    // but it will also increase CPU consumption. As CPU usage increases and there
    // is more contention between the concurrent function calls, client-side latencies
    // may increase.
    // Note: You are likely to see degraded performance if you increase this above 50
    // and observe elevated client-side latencies.
    final int numberOfConcurrentRequests = 100;
    //
    // Sets an upper bound on how many requests per second will be sent to the server.
    // Momento caches have a default throttling limit of 100 requests per second,
    // so if you raise this, you may observe throttled requests. Contact
    // support@momentohq.com to inquire about raising your limits.
    //
    final int maxRequestsPerSecond = 100;
    //
    // Controls how long the load test will run.
    //
    final int howLongToRunSeconds = 60;
    //
    // Controls how long the load generator will run before resetting the histogram.
    // Removes outlier times due to client connection or code loading/jit.
    final int warmupTimeSeconds = 10;

    final LoadGeneratorSet loadGenerator =
        new LoadGeneratorSet(
            showStatsInterval,
            numberOfConcurrentRequests,
            maxRequestsPerSecond,
            cacheItemPayloadBytes,
            warmupTimeSeconds);

    // Wait for the desired time
    Thread.sleep(howLongToRunSeconds * 1000);

    loadGenerator.shutdown();

    Thread.sleep(5000);
  }
}
