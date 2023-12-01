package momento.client.example;

import com.google.common.util.concurrent.RateLimiter;
import java.time.Duration;
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
  private static final String CACHE_NAME = "java-loadgen";
  private final CacheClient client;
  private final LongAdder globalRequestCount = new LongAdder();
  private final LongAdder globalSuccessCount = new LongAdder();
  private final LongAdder globalErrorCount = new LongAdder();
  private final LongAdder globalThrottleCount = new LongAdder();

  private final ExecutorService executorService;

  public BasicSetLoad() {
    this.executorService = Executors.newFixedThreadPool(10);
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

    final CacheCreateResponse cresponse = this.client.createCache(CACHE_NAME).join();
    if (cresponse instanceof CacheCreateResponse.Error) {
      throw new RuntimeException(((CacheCreateResponse.Error) cresponse).getMessage());
    }

    final long testEndTime =
        System.currentTimeMillis() + Duration.ofMinutes(TEST_DURATION_MINUTES).toMillis();

    final RateLimiter rateLimiter = RateLimiter.create(100);

    // Scheduled task for printing histogram
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutor.scheduleAtFixedRate(
        () -> printData(),
        HISTOGRAM_PRINT_INTERVAL_SECONDS,
        HISTOGRAM_PRINT_INTERVAL_SECONDS,
        TimeUnit.SECONDS);

    // Submitting tasks for the duration of the test
    while (System.currentTimeMillis() < testEndTime) {
      this.executorService.submit(
          () -> {
            final String key = "key" + Thread.currentThread().getId();
            final String value = "x".repeat(200);
            rateLimiter.acquire();
            final long startTime = System.nanoTime();
            final SetResponse response = this.client.set(CACHE_NAME, key, value).join();
            final long endTime = System.nanoTime();
            this.setHistogram.recordValue(endTime - startTime);
            this.globalRequestCount.increment();
            if (response instanceof SetResponse.Success) {
              this.globalSuccessCount.increment();
            } else if (response instanceof SetResponse.Error) {
              MomentoErrorCode errorCode = ((SetResponse.Error) response).getErrorCode();
              if (errorCode.equals(MomentoErrorCode.LIMIT_EXCEEDED_ERROR)) {
                this.globalThrottleCount.increment();
              } else {

                System.out.println(((SetResponse.Error) response).getMessage());
              }
              this.globalErrorCount.increment();
            }
          });
    }

    // Shutting down executors after the test duration
    this.executorService.shutdown();
    scheduledExecutor.shutdown();
    try {
      if (!this.executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        this.executorService.shutdownNow();
      }
      if (!scheduledExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
        scheduledExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      this.executorService.shutdownNow();
      scheduledExecutor.shutdownNow();
    }
  }

  private void printData() {
    StringBuilder builder = new StringBuilder();
    builder.append("\n--- Histogram Data ---\n");

    // Printing the histogram for latencies
    builder.append("Latency (in microseconds):\n");
    builder.append(formatHistogram(setHistogram));

    // Printing the counts for success and error
    builder.append("\n--- Request Counts ---\n");
    builder.append(String.format("Total Requests: %d\n", globalRequestCount.sum()));
    builder.append(String.format("Success Count: %d\n", globalSuccessCount.sum()));
    builder.append(String.format("Error Count: %d\n", globalErrorCount.sum()));
    builder.append(String.format("Throttle Count: %d\n", globalThrottleCount.sum()));

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
