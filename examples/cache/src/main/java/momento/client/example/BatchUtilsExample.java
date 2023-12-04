package momento.client.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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

/**
 * BatchUtilsExample demonstrates the use of MomentoBatchUtils for performing batch operations with
 * the Momento SDK. This class includes examples of setting up a cache, adding test data, and
 * performing batch get operations.
 *
 * <p>The example covers: - Creating a cache if it does not exist. - Adding a set of key-value pairs
 * to the cache. - Performing a batch get operation to retrieve multiple keys in a single call. -
 * Handling responses and displaying the results.
 *
 * <p>It utilizes the Momento SDK's CacheClient for cache operations and the MomentoBatchUtils for
 * handling batch operations efficiently.
 */
public class BatchUtilsExample {

  private static final String API_KEY_ENV_VAR = "MOMENTO_API_KEY";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  private static final String CACHE_NAME = "cache";

  // represents the concurrency level for the batch of requests we send to Momento
  private static final int MAX_CONCURRENT_REQUESTS = 20;

  private static final int TOTAL_KEYS = 100;

  public static void main(String[] args) {

    final CredentialProvider credentialProvider = new EnvVarCredentialProvider(API_KEY_ENV_VAR);

    try (final CacheClient client =
        CacheClient.create(credentialProvider, Configurations.Laptop.latest(), DEFAULT_ITEM_TTL)) {

      createCache(client, CACHE_NAME);

      printStartBanner();

      setupTestData(client);
      performBatchGet(client);

      printEndBanner();
    }
  }

  private static void setupTestData(CacheClient cacheClient) {
    for (int i = 0; i < TOTAL_KEYS; i++) {
      final String key = "key" + i;
      final String value = "value" + i;
      cacheClient.set(CACHE_NAME, key, value).join();
      System.out.println("Added " + key + " -> " + value);
    }
  }

  private static void performBatchGet(final CacheClient client) {

    // Building the util client using try-with-resources semantic to auto-close the client. Doing so
    // shuts down
    // and cleans up the executor
    try (final MomentoBatchUtils momentoBatchUtils =
        MomentoBatchUtils.builder(client)
            .withMaxConcurrentRequests(MAX_CONCURRENT_REQUESTS)
            .build()) {

      final List<String> keys = new ArrayList<>();

      for (int i = 0; i < TOTAL_KEYS; i++) {
        final String key = "key" + i;
        keys.add(key);
      }

      final BatchGetRequest.StringKeyBatchGetRequest request =
          new BatchGetRequest.StringKeyBatchGetRequest(keys);

      System.out.println("Performing batchGet for " + TOTAL_KEYS + " number of keys");

      final BatchGetResponse response = momentoBatchUtils.batchGet(CACHE_NAME, request).join();

      if (response instanceof BatchGetResponse.StringKeyBatchGetSummary summary) {
        for (BatchGetResponse.StringKeyBatchGetSummary.GetSummary getSummary :
            summary.getSummaries()) {
          System.out.println(
              "Key: "
                  + getSummary.getKey()
                  + ", Value: "
                  + ((GetResponse.Hit) getSummary.getGetResponse()).valueString());
        }
      } else {
        System.out.println("Error occurred during batch get operation.");
      }
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

  private static void printStartBanner() {
    System.out.println("******************************************************************");
    System.out.println("BatchUtils Example Start");
    System.out.println("******************************************************************");
  }

  private static void printEndBanner() {
    System.out.println("******************************************************************");
    System.out.println("BatchUtils Example End");
    System.out.println("******************************************************************");
  }
}
