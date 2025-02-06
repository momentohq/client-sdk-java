package momento.sdk.retry.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import momento.sdk.retry.MomentoRpcMethod;

public class TestRetryMetricsCollector {
  // Data structure to store timestamps: cacheName -> requestName -> [timestamps]
  private final Map<String, Map<MomentoRpcMethod, List<Long>>> data;

  public TestRetryMetricsCollector() {
    this.data = new HashMap<>();
  }

  /**
   * Adds a timestamp for a specific request and cache.
   *
   * @param cacheName - The name of the cache.
   * @param requestName - The name of the request (using MomentoRPCMethod enum).
   * @param timestamp - The timestamp to record in seconds since epoch.
   */
  public void addTimestamp(String cacheName, MomentoRpcMethod requestName, long timestamp) {
    data.computeIfAbsent(cacheName, k -> new HashMap<>())
        .computeIfAbsent(requestName, k -> new ArrayList<>())
        .add(timestamp);
  }

  /**
   * Calculates the total retry count for a specific cache and request.
   *
   * @param cacheName - The name of the cache.
   * @param requestName - The name of the request (using MomentoRPCMethod enum).
   * @return The total number of retries.
   */
  public int getTotalRetryCount(String cacheName, MomentoRpcMethod requestName) {
    List<Long> timestamps =
        data.getOrDefault(cacheName, new HashMap<>()).getOrDefault(requestName, new ArrayList<>());
    // Number of retries is one less than the number of timestamps.
    return Math.max(0, timestamps.size() - 1);
  }

  /**
   * Calculates the average time between retries for a specific cache and request.
   *
   * @param cacheName - The name of the cache.
   * @param requestName - The name of the request (using MomentoRPCMethod enum).
   * @return The average time in seconds, or `0` if there are no retries.
   */
  public double getAverageTimeBetweenRetries(String cacheName, MomentoRpcMethod requestName) {
    List<Long> timestamps =
        data.getOrDefault(cacheName, new HashMap<>()).getOrDefault(requestName, new ArrayList<>());
    if (timestamps.size() < 2) {
      return 0.0; // No retries occurred.
    }
    long totalInterval = 0;
    for (int i = 1; i < timestamps.size(); i++) {
      totalInterval += (timestamps.get(i) - timestamps.get(i - 1));
    }
    return (double) totalInterval / (timestamps.size() - 1);
  }

  /**
   * Retrieves all collected metrics for debugging or analysis.
   *
   * @return The complete data structure with all recorded metrics.
   */
  public Map<String, Map<MomentoRpcMethod, List<Long>>> getAllMetrics() {
    return data;
  }
}
