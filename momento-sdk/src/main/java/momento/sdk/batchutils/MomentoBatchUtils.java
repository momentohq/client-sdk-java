package momento.sdk.batchutils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import momento.sdk.CacheClient;
import momento.sdk.batchutils.request.BatchGetRequest;
import momento.sdk.batchutils.response.BatchGetResponse;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.responses.cache.GetResponse;

/** Utility class for handling batch operations in Momento SDK. */
public class MomentoBatchUtils {

  private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 5;

  private static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 10;

  private static final int THREAD_POOL_KEEP_ALIVE_TTL_SECONDS = 60;

  private final CacheClient cacheClient;

  private final int maxConcurrentRequests;

  private final int requestTimeoutSeconds;

  private final ExecutorService executorService;

  /**
   * Constructs a MomentoBatchUtils instance.
   *
   * @param cacheClient The cache client used for cache operations.
   * @param maxConcurrentRequests The maximum number of concurrent requests.
   */
  private MomentoBatchUtils(
      final CacheClient cacheClient,
      final int maxConcurrentRequests,
      final int requestTimeoutSeconds) {
    this.cacheClient = cacheClient;
    this.maxConcurrentRequests = maxConcurrentRequests;
    this.requestTimeoutSeconds = requestTimeoutSeconds;
    this.executorService =
        new ThreadPoolExecutor(
            maxConcurrentRequests,
            maxConcurrentRequests,
            THREAD_POOL_KEEP_ALIVE_TTL_SECONDS,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>());
  }

  public static class MomentoBatchUtilsBuilder {

    private final CacheClient cacheClient;

    private int requestTimeoutSeconds = DEFAULT_REQUEST_TIMEOUT_SECONDS;

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
     * Sets the maximum timeout for each individual asynchronous request that is sent to Momento. In
     * theory these requests should never take any disk or network time as it's simply submitting an
     * async request, so the default 10 second timeout is a more than conservative amount for the
     * same.
     *
     * @param requestTimeoutSeconds The maximum timeout for each individual request to Momento.
     * @return The builder instance for chaining.
     */
    public MomentoBatchUtilsBuilder withRequestTimeoutSeconds(int requestTimeoutSeconds) {
      this.requestTimeoutSeconds = requestTimeoutSeconds;
      return this;
    }

    /**
     * Builds and returns a MomentoBatchUtils instance.
     *
     * @return A new instance of MomentoBatchUtils.
     */
    public MomentoBatchUtils build() {
      return new MomentoBatchUtils(cacheClient, maxConcurrentRequests, requestTimeoutSeconds);
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
   * @return BatchGetResponse The batch get response
   */
  public BatchGetResponse batchGet(
      final String cacheName, final BatchGetRequest.StringKeyBatchGetRequest request) {

    final List<BatchGetResponse.StringKeyBatchGetSummary.GetSummary> summaries = new ArrayList<>();

    for (final String key : request.getKeys()) {
      final CompletableFuture<GetResponse> getResponseFuture;
      try {
        getResponseFuture =
            executorService
                .submit(() -> cacheClient.get(cacheName, key))
                .get(this.requestTimeoutSeconds, TimeUnit.SECONDS);
      } catch (Exception e) {
        return new BatchGetResponse.Error(CacheServiceExceptionMapper.convert(e.getCause()));
      }
      summaries.add(
          new BatchGetResponse.StringKeyBatchGetSummary.GetSummary(key, getResponseFuture));
    }

    return new BatchGetResponse.StringKeyBatchGetSummary(summaries);
  }

  /**
   * Performs a batch get operation for byte array keys.
   *
   * @param cacheName The name of the cache.
   * @param request The batch get request with byte array keys.
   * @return BatchGetResponse The batch get response
   */
  public BatchGetResponse batchGet(
      final String cacheName, final BatchGetRequest.ByteArrayKeyBatchGetRequest request) {

    final List<BatchGetResponse.ByteArrayKeyBatchGetSummary.GetSummary> summaries =
        new ArrayList<>();

    for (final byte[] key : request.getKeys()) {
      final CompletableFuture<GetResponse> getResponseFuture;
      try {
        getResponseFuture =
            executorService
                .submit(() -> cacheClient.get(cacheName, key))
                .get(this.requestTimeoutSeconds, TimeUnit.SECONDS);
      } catch (Exception e) {
        return new BatchGetResponse.Error(CacheServiceExceptionMapper.convert(e.getCause()));
      }
      summaries.add(
          new BatchGetResponse.ByteArrayKeyBatchGetSummary.GetSummary(key, getResponseFuture));
    }

    return new BatchGetResponse.ByteArrayKeyBatchGetSummary(summaries);
  }
}
