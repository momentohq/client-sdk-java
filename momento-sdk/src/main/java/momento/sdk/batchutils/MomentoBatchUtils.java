package momento.sdk.batchutils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import momento.sdk.CacheClient;
import momento.sdk.batchutils.request.BatchGetRequest;
import momento.sdk.batchutils.response.BatchGetResponse;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.responses.cache.GetResponse;

/** Utility class for handling batch operations in Momento SDK. */
public class MomentoBatchUtils {

  private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 5;

  private final CacheClient cacheClient;

  private final int maxConcurrentRequests;

  /**
   * Constructs a MomentoBatchUtils instance.
   *
   * @param cacheClient The cache client used for cache operations.
   * @param maxConcurrentRequests The maximum number of concurrent requests.
   */
  private MomentoBatchUtils(final CacheClient cacheClient, final int maxConcurrentRequests) {
    this.cacheClient = cacheClient;
    this.maxConcurrentRequests = maxConcurrentRequests;
  }

  public static class MomentoBatchUtilsBuilder {

    private final CacheClient cacheClient;

    private int maxConcurrentRequests = DEFAULT_MAX_CONCURRENT_REQUESTS; // default value

    /**
     * Creates a builder for MomentoBatchUtils.
     *
     * @param cacheClient The CacheClient to be used by MomentoBatchUtils.
     */
    public MomentoBatchUtilsBuilder(final CacheClient cacheClient) {
      this.cacheClient = cacheClient;
    }

    /**
     * Sets the maximum number of concurrent requests allowed in a batch operation. This limit is
     * important for controlling the load on both the client and server side. Increasing this number
     * can lead to a higher network and server stress. It is advised to keep this number reasonable
     * and not exceed a certain threshold (e.g., 100), as higher values may significantly impact the
     * application's performance and stability, particularly under constrained network conditions or
     * limited server resources.
     *
     * @param maxConcurrentRequests The maximum number of concurrent requests. Caution should be
     *     exercised if setting this value above 100.
     * @return The builder instance for chaining.
     */
    public MomentoBatchUtilsBuilder withMaxConcurrentRequests(int maxConcurrentRequests) {
      this.maxConcurrentRequests = maxConcurrentRequests;
      return this;
    }

    /**
     * Builds and returns a MomentoBatchUtils instance.
     *
     * @return A new instance of MomentoBatchUtils.
     */
    public MomentoBatchUtils build() {
      return new MomentoBatchUtils(cacheClient, maxConcurrentRequests);
    }
  }

  public static MomentoBatchUtilsBuilder builder(final CacheClient cacheClient) {
    return new MomentoBatchUtilsBuilder(cacheClient);
  }

  /**
   * Performs a batch get operation for String keys.
   *
   * @param cacheName The name of the cache.
   * @param request The batch get request with String keys.
   * @return A CompletableFuture of BatchGetResponse.
   */
  public BatchGetResponse batchGet(
      final String cacheName, final BatchGetRequest.StringKeyBatchGetRequest request) {
    return batchGetInternal(cacheName, request.getKeys(), String::valueOf);
  }

  /**
   * Performs a batch get operation for byte array keys.
   *
   * @param cacheName The name of the cache.
   * @param request The batch get request with byte array keys.
   * @return A CompletableFuture of BatchGetResponse.
   */
  public BatchGetResponse batchGet(
      final String cacheName, final BatchGetRequest.ByteArrayKeyBatchGetRequest request) {
    return batchGetInternal(
        cacheName, request.getKeys(), key -> new String(key, StandardCharsets.UTF_8));
  }

  /**
   * Internal method to handle the common batch get logic.
   *
   * @param cacheName The name of the cache.
   * @param keys The collection of keys to get.
   * @param keyMapper Function to map the key to a String representation.
   * @return A CompletableFuture of BatchGetResponse.
   */
  private <T> BatchGetResponse batchGetInternal(
      final String cacheName, final Collection<T> keys, final Function<T, String> keyMapper) {

    if (keys.size() > this.maxConcurrentRequests) {
      return new BatchGetResponse.Error(
          CacheServiceExceptionMapper.convert(
              new InvalidArgumentException(
                  String.format(
                      "Number of keys should be less than "
                          + "or equal to maxConcurrentRequests. You can configure this value using MomentoBatchUtilsBuilder "
                          + "option withMaxConcurrentRequests. Current value for maxConcurrentRequests: %d",
                      this.maxConcurrentRequests))));
    }

    final List<BatchGetResponse.BatchGetSummary.GetSummary> summaries = new ArrayList<>();

    for (final T key : keys) {
      final String keyStr = keyMapper.apply(key);
      CompletableFuture<GetResponse> getResponseFuture = cacheClient.get(cacheName, keyStr);
      summaries.add(new BatchGetResponse.BatchGetSummary.GetSummary(keyStr, getResponseFuture));
    }

    return new BatchGetResponse.BatchGetSummary(summaries);
  }
}
