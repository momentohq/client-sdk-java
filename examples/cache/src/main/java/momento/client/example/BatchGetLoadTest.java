package momento.client.example;

import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.batchutils.MomentoBatchUtils;
import momento.sdk.batchutils.request.BatchGetRequest;
import momento.sdk.batchutils.response.BatchGetResponse;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import org.HdrHistogram.ConcurrentHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchGetLoadTest {

  private static final Logger logger = LoggerFactory.getLogger(BatchGetLoadTest.class);

  private static final String API_KEY_ENV_VAR = "MOMENTO_API_KEY";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);
  private static final ConcurrentHistogram batchGetHistogram = new ConcurrentHistogram(3);

  private static final String CACHE_NAME = "cache";

  private static final LongAdder batchGetSuccesses = new LongAdder();
  private static final LongAdder batchGetErrors = new LongAdder();
  private static final LongAdder batchGetIndividualHits = new LongAdder();
  private static final LongAdder batchGetIndividualMisses = new LongAdder();
  private static final LongAdder batchGetIndividualErrors = new LongAdder();

  public static void main(String[] args) {

    final CredentialProvider credentialProvider = new EnvVarCredentialProvider(API_KEY_ENV_VAR);

    try (final CacheClient client =
        CacheClient.create(credentialProvider, Configurations.Laptop.latest(), DEFAULT_ITEM_TTL)) {

      createCache(client, CACHE_NAME);

      try (final MomentoBatchUtils momentoBatchUtils = MomentoBatchUtils.builder(client).build()) {
        setupTestData(client, CACHE_NAME);
        performBatchGet(momentoBatchUtils, CACHE_NAME);
      }
    }
  }

  private static void performBatchGet(MomentoBatchUtils batchUtils, String cacheName) {

    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      final String key = "key" + i;
      keys.add(key);
    }

    List<List<String>> allKeys = Lists.partition(keys, 10);

    for (List<String> keyBatch : allKeys) {
      final BatchGetRequest.StringKeyBatchGetRequest request =
          new BatchGetRequest.StringKeyBatchGetRequest(keyBatch);

      final long startTime = System.nanoTime();
      final BatchGetResponse response = batchUtils.batchGet(cacheName, request).join();
      final long endTime = System.nanoTime();
      batchGetHistogram.recordValue(endTime - startTime);
      if (response instanceof BatchGetResponse.StringKeyBatchGetSummary summary) {
        batchGetSuccesses.increment();
        for (BatchGetResponse.StringKeyBatchGetSummary.GetSummary getSummary :
            summary.getSummaries()) {
          if (getSummary.getGetResponse() instanceof GetResponse.Error) {
            batchGetIndividualErrors.increment();
          } else if (getSummary.getGetResponse() instanceof GetResponse.Hit) {
            batchGetIndividualHits.increment();
          } else if (getSummary.getGetResponse() instanceof GetResponse.Miss) {
            batchGetIndividualMisses.increment();
          }
        }
      } else {
        batchGetErrors.increment();
      }
    }
  }

  private static void setupTestData(CacheClient cacheClient, String cacheName) {
    final String val = "x".repeat(200000);
    for (int i = 0; i < 1000; i++) {
      final String key = "key" + i;
      cacheClient.set(cacheName, "key" + i, val).join();
      System.out.println("Added " + key + " -> " + val);
    }
  }

  private static void createCache(CacheClient cacheClient, String cacheName) {
    final CacheCreateResponse createResponse = cacheClient.createCache(cacheName).join();
    if (createResponse instanceof CacheCreateResponse.Error error) {
      if (error.getCause() instanceof AlreadyExistsException) {
        System.out.println("Cache with name '" + cacheName + "' already exists.");
      } else {
        System.out.println("Unable to create cache with error " + error.getErrorCode());
        System.out.println(error.getMessage());
      }
    }
  }

  private void printBatchGetData() {
    StringBuilder builder = new StringBuilder();
    builder.append("\n--- Delete Operation Data ---\n");
    builder.append("Delete Latency (in millis):\n").append(formatHistogram(batchGetHistogram));
    builder.append(String.format("BatchGet Total Requests: %d\n", batchGetSuccesses.sum()));
    builder.append(
        String.format("BatchGet Individual Hit Count: %d\n", batchGetIndividualHits.sum()));
    builder.append(
        String.format("BatchGet Individual Miss Count: %d\n", batchGetIndividualMisses.sum()));

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
}
