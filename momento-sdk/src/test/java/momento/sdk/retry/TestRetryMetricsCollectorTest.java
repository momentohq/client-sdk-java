package momento.sdk.retry;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestRetryMetricsCollectorTest {

  @Test
  public void testAddTimestamp() {
    TestRetryMetricsCollector collector = new TestRetryMetricsCollector();
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 1000);

    Map<String, Map<MomentoRpcMethod, List<Long>>> metrics = collector.getAllMetrics();
    assertTrue(metrics.containsKey("cache1"));
    assertTrue(metrics.get("cache1").containsKey(MomentoRpcMethod.GET));
    assertEquals(1, metrics.get("cache1").get(MomentoRpcMethod.GET).size());
    assertEquals(1000L, metrics.get("cache1").get(MomentoRpcMethod.GET).get(0));
  }

  @Test
  public void testGetTotalRetryCount_NoRetries() {
    TestRetryMetricsCollector collector = new TestRetryMetricsCollector();
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 1000);

    int retryCount = collector.getTotalRetryCount("cache1", MomentoRpcMethod.GET);
    assertEquals(0, retryCount);
  }

  @Test
  public void testGetTotalRetryCount_WithRetries() {
    TestRetryMetricsCollector collector = new TestRetryMetricsCollector();
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 1000);
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 2000);
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 3000);

    int retryCount = collector.getTotalRetryCount("cache1", MomentoRpcMethod.GET);
    assertEquals(2, retryCount);
  }

  @Test
  public void testGetAverageTimeBetweenRetries_NoRetries() {
    TestRetryMetricsCollector collector = new TestRetryMetricsCollector();
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 1000);

    double averageTime = collector.getAverageTimeBetweenRetries("cache1", MomentoRpcMethod.GET);
    assertEquals(0.0, averageTime);
  }

  @Test
  public void testGetAverageTimeBetweenRetries_WithRetries() {
    TestRetryMetricsCollector collector = new TestRetryMetricsCollector();
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 1000);
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 2000);
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 4000);

    double averageTime = collector.getAverageTimeBetweenRetries("cache1", MomentoRpcMethod.GET);
    assertEquals(1500.0, averageTime);
  }

  @Test
  public void testGetAllMetrics() {
    TestRetryMetricsCollector collector = new TestRetryMetricsCollector();
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 1000);
    collector.addTimestamp("cache1", MomentoRpcMethod.GET, 2000);
    collector.addTimestamp("cache2", MomentoRpcMethod.SET, 3000);

    Map<String, Map<MomentoRpcMethod, List<Long>>> metrics = collector.getAllMetrics();
    System.out.println(metrics);
    assertNotNull(metrics);
    Map<String, Map<MomentoRpcMethod, List<Long>>> expectedMetrics =
        Map.of(
            "cache1", Map.of(MomentoRpcMethod.GET, List.of(1000L, 2000L)),
            "cache2", Map.of(MomentoRpcMethod.SET, List.of(3000L)));
    assertEquals(expectedMetrics, metrics);
  }

  @Test
  public void testNoData() {
    TestRetryMetricsCollector collector = new TestRetryMetricsCollector();
    assertEquals(0, collector.getTotalRetryCount("cache1", MomentoRpcMethod.GET));
    assertEquals(0, collector.getTotalRetryCount("cache1", MomentoRpcMethod.GET));
    assertEquals(0.0, collector.getAverageTimeBetweenRetries("cache1", MomentoRpcMethod.GET));
    assertTrue(collector.getAllMetrics().isEmpty());
  }
}
