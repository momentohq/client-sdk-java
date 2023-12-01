package momento.client.example;

import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import org.HdrHistogram.ConcurrentHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicSetLoad {

  private static final Logger logger = LoggerFactory.getLogger(BasicSetLoad.class);

  private static final long TEST_DURATION_MINUTES = 1;
  private static final long HISTOGRAM_PRINT_INTERVAL_SECONDS = 5;

  private static final String API_KEY_ENV_VAR = "MOMENTO_API_KEY";

  private final ConcurrentHistogram setHistogram = new ConcurrentHistogram(3);
  private final ConcurrentHistogram getHistogram = new ConcurrentHistogram(3);
  private final ConcurrentHistogram deleteHistogram = new ConcurrentHistogram(3);
  private static final String CACHE_NAME = "java-loadgen";
  private final CacheClient client;
  private final LongAdder globalRequestCount = new LongAdder();
  private final LongAdder globalSuccessCount = new LongAdder();
  private final LongAdder globalErrorCount = new LongAdder();
  private final LongAdder globalThrottleCount = new LongAdder();

  private final LongAdder globalGetHitsCount = new LongAdder();

  private final LongAdder globalDeleteSuccessCount = new LongAdder();
  private final LongAdder globalDeleteErrorCount = new LongAdder();
  private final LongAdder globalGetMissesCount = new LongAdder();
  private final LongAdder globalGetErrorCount = new LongAdder();


  private final ExecutorService executorService;

  public BasicSetLoad() {
    this.executorService = Executors.newFixedThreadPool(100);
    final CredentialProvider credentialProvider;
    try {
      credentialProvider = new EnvVarCredentialProvider(API_KEY_ENV_VAR);
    } catch (SdkException e) {
      logger.error("Unable to load credential from environment variable " + API_KEY_ENV_VAR, e);
      throw e;
    }

    client =
        CacheClient.create(
            credentialProvider, Configurations.Laptop.v1(), Duration.ofSeconds(1000));
  }

  public void run() {


    final long testEndTime =
        System.currentTimeMillis() + Duration.ofMinutes(TEST_DURATION_MINUTES).toMillis();

    final RateLimiter rateLimiter = RateLimiter.create(100);

    // Scheduled task for printing histogram
    ScheduledExecutorService setScheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    setScheduledExecutor.scheduleAtFixedRate(
        () -> printSetData(),
        HISTOGRAM_PRINT_INTERVAL_SECONDS,
        HISTOGRAM_PRINT_INTERVAL_SECONDS,
        TimeUnit.SECONDS);

    final List<String> keys = new ArrayList<>();
    // Submitting tasks for the duration of the test
    // set test
    while (System.currentTimeMillis() < testEndTime) {
      rateLimiter.acquire();
      this.executorService.submit(
          () -> {
            final String key = "key" + Thread.currentThread().getId();
            keys.add(key);
            final String value = "x".repeat(200);
            final long startTime = System.nanoTime();
            final SetResponse response = this.client.set(CACHE_NAME, key, value).join();
            final long endTime = System.nanoTime();
            this.setHistogram.recordValue(endTime - startTime);
            this.globalRequestCount.increment();
            if (response instanceof SetResponse.Success) {
              this.globalSuccessCount.increment();
            } else if (response instanceof SetResponse.Error) {
              this.globalErrorCount.increment();
            }
          });
    }

    setScheduledExecutor.shutdown();
    try {
      if (!setScheduledExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        setScheduledExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      setScheduledExecutor.shutdownNow();
    }

    ScheduledExecutorService getScheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    getScheduledExecutor.scheduleAtFixedRate(
            () -> printGetData(),
            HISTOGRAM_PRINT_INTERVAL_SECONDS,
            HISTOGRAM_PRINT_INTERVAL_SECONDS,
            TimeUnit.SECONDS);

    // Submitting tasks for the duration of the test
    // get test
    Random random = new Random();
    while (System.currentTimeMillis() < testEndTime) {
      rateLimiter.acquire();
      this.executorService.submit(() -> {
        if (!keys.isEmpty()) {
          final String key = keys.get(random.nextInt(keys.size()));
          final long startTime = System.nanoTime();
          final GetResponse response = this.client.get(CACHE_NAME, key).join();
          final long endTime = System.nanoTime();
          this.getHistogram.recordValue(endTime - startTime);
          this.globalRequestCount.increment();
          if (response instanceof GetResponse.Hit) {
            this.globalSuccessCount.increment();
            this.globalGetHitsCount.increment();
          } else if (response instanceof GetResponse.Miss) {
            this.globalSuccessCount.increment();
            this.globalGetMissesCount.increment();
          } else {
            this.globalGetErrorCount.increment();
          }
        }
      });
    }

    getScheduledExecutor.shutdown();
    try {
      if (!getScheduledExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        getScheduledExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      getScheduledExecutor.shutdownNow();
    }


    ScheduledExecutorService deleteScheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    deleteScheduledExecutor.scheduleAtFixedRate(
            () -> printDeleteData(),
            HISTOGRAM_PRINT_INTERVAL_SECONDS,
            HISTOGRAM_PRINT_INTERVAL_SECONDS,
            TimeUnit.SECONDS);

    // delete test
    int index = 0;
    while (index < keys.size()) {
      int endIndex = Math.min(index + 100, keys.size());
      List<String> batchKeys = keys.subList(index, endIndex);
      batchKeys.stream().parallel().forEach(key -> {
        rateLimiter.acquire();
        this.executorService.submit(() -> {
          final long startTime = System.nanoTime();
          final DeleteResponse response = this.client.delete(CACHE_NAME, key).join();
          final long endTime = System.nanoTime();
          this.deleteHistogram.recordValue(endTime - startTime);
          this.globalRequestCount.increment();
          if (response instanceof DeleteResponse.Success) {
            this.globalSuccessCount.increment();
            this.globalDeleteSuccessCount.increment();
          } else {
            this.globalErrorCount.increment();
            this.globalDeleteErrorCount.increment();
          }
        });
      });
      index = endIndex;
    }

    deleteScheduledExecutor.shutdown();
    try {
      if (!deleteScheduledExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        deleteScheduledExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      deleteScheduledExecutor.shutdownNow();
    }


    // Shutting down executors after the test duration
    this.executorService.shutdown();

    try {
      if (!this.executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        this.executorService.shutdownNow();
      }

    } catch (InterruptedException e) {
      this.executorService.shutdownNow();
    }
  }

  private void printSetData() {
    StringBuilder builder = new StringBuilder();
    builder.append("\n--- Histogram Data ---\n");

    // Printing the histogram for latencies
    builder.append("Latency (in millis):\n");
    builder.append(formatHistogram(setHistogram));

    // Printing the counts for success and error
    builder.append("\n--- Request Counts ---\n");
    builder.append(String.format("Total Requests: %d\n", globalRequestCount.sum()));
    builder.append(String.format("Success Count: %d\n", globalSuccessCount.sum()));
    builder.append(String.format("Error Count: %d\n", globalErrorCount.sum()));
    builder.append(String.format("Throttle Count: %d\n", globalThrottleCount.sum()));

    logger.info(builder.toString());
  }

  private void printGetData() {
    StringBuilder builder = new StringBuilder();
    builder.append("\n--- Get Operation Data ---\n");
    builder.append("Get Latency (in millis):\n").append(formatHistogram(getHistogram));
    builder.append(String.format("Get Hits Count: %d\n", globalGetHitsCount.sum()));
    builder.append(String.format("Get Misses Count: %d\n", globalGetMissesCount.sum()));
    builder.append(String.format("Get Error Count: %d\n", globalGetErrorCount.sum()));
    logger.info(builder.toString());
  }

  private void printDeleteData() {
    StringBuilder builder = new StringBuilder();
    builder.append("\n--- Delete Operation Data ---\n");
    builder.append("Delete Latency (in millis):\n").append(formatHistogram(deleteHistogram));
    builder.append(String.format("Delete Success Count: %d\n", globalDeleteSuccessCount.sum()));
    builder.append(String.format("Delete Error Count: %d\n", globalDeleteErrorCount.sum()));
    logger.info(builder.toString());
  }

  private String formatHistogram(ConcurrentHistogram histogram) {
    return String.format("Count: %d\n", histogram.getTotalCount())
        + String.format("Min: %.2f ms\n", histogram.getMinValue() / 1_000_000.0)
        + String.format("p50: %.2f ms\n", histogram.getValueAtPercentile(50.0) / 1_000_000.0)
        + String.format("p90: %.2f ms\n", histogram.getValueAtPercentile(90.0) / 1_000_000.0)
        + String.format("p95: %.2f ms\n", histogram.getValueAtPercentile(95.0) / 1_000_000.0)
        + String.format("p99: %.2f ms\n", histogram.getValueAtPercentile(99.0) / 1_000_000.0)
        + String.format("p99.9: %.2f ms\n", histogram.getValueAtPercentile(99.9) / 1_000_000.0)
        + String.format("Max: %.2f ms\n", histogram.getMaxValue() / 1_000_000.0);
  }

  public static void main(String... args) {
    final BasicSetLoad basicSetLoad = new BasicSetLoad();
    basicSetLoad.run();
  }
}
