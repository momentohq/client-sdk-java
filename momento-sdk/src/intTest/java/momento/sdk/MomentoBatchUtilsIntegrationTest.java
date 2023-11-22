package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import momento.sdk.batchutils.MomentoBatchUtils;
import momento.sdk.batchutils.request.BatchGetRequest;
import momento.sdk.batchutils.response.BatchGetResponse;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.GetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MomentoBatchUtilsIntegrationTest extends BaseTestClass {

  private CacheClient cacheClient;
  private String cacheName;
  private MomentoBatchUtils momentoBatchUtils;

  @BeforeEach
  void setup() {
    cacheClient =
        CacheClient.builder(
                credentialProvider, Configurations.Laptop.latest(), Duration.ofSeconds(10))
            .build();
    cacheName = "batchTest";
    cacheClient.createCache(cacheName).join();

    momentoBatchUtils = MomentoBatchUtils.builder(cacheClient).build();
  }

  @AfterEach
  void teardown() {
    cacheClient.deleteCache(cacheName).join();
    cacheClient.close();
  }

  @Test
  void testBatchGetWithStringKeys() throws ExecutionException, InterruptedException {
    // Setup test data
    String key1 = "testKey1";
    String value1 = "testValue1";
    cacheClient.set(cacheName, key1, value1).join();

    String key2 = "testKey2";
    String value2 = "testValue2";
    cacheClient.set(cacheName, key2, value2).join();

    // Create a batch get request
    BatchGetRequest.StringKeyBatchGetRequest request =
        new BatchGetRequest.StringKeyBatchGetRequest(Arrays.asList(key1, key2));

    // Perform batch get
    BatchGetResponse response = momentoBatchUtils.batchGet(cacheName, request);

    // Assertions
    assertThat(response).isNotNull();
    assertThat(response).isInstanceOf(BatchGetResponse.BatchGetSummary.class);
    List<BatchGetResponse.BatchGetSummary.GetSummary> summaries =
        ((BatchGetResponse.BatchGetSummary) response).getSummaries();
    assertThat(summaries).hasSize(2);

    // Assert each response
    for (BatchGetResponse.BatchGetSummary.GetSummary summary : summaries) {
      String key = summary.getKey();
      GetResponse getResponse = summary.getGetResponse().join();

      assertThat(key).isIn(key1, key2);

      if (key.equals(key1)) {
        validateResponse(getResponse, value1);
      } else if (key.equals(key2)) {
        validateResponse(getResponse, value2);
      } else {
        fail("Unexpected key encountered in the response");
      }
    }
  }

  private void validateResponse(GetResponse getResponse, String expectedValue) {
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    GetResponse.Hit hitResponse = (GetResponse.Hit) getResponse;
    String retrievedValue = hitResponse.valueString();
    assertThat(retrievedValue).isEqualTo(expectedValue);
  }

  @Test
  void testBatchGetWithByteArrayKeys() throws ExecutionException, InterruptedException {
    // Setup test data
    byte[] key1 = "testKey1".getBytes();
    byte[] value1 = "testValue1".getBytes();
    cacheClient.set(cacheName, key1, value1).join();

    byte[] key2 = "testKey2".getBytes();
    byte[] value2 = "testValue2".getBytes();
    cacheClient.set(cacheName, key2, value2).join();

    // Create a batch get request
    BatchGetRequest.ByteArrayKeyBatchGetRequest request =
        new BatchGetRequest.ByteArrayKeyBatchGetRequest(Arrays.asList(key1, key2));

    // Perform batch get
    BatchGetResponse response = momentoBatchUtils.batchGet(cacheName, request);

    // Assertions
    assertThat(response).isNotNull();
    assertThat(response).isInstanceOf(BatchGetResponse.BatchGetSummary.class);
    List<BatchGetResponse.BatchGetSummary.GetSummary> summaries =
        ((BatchGetResponse.BatchGetSummary) response).getSummaries();
    assertThat(summaries).hasSize(2);

    // Assert each response
    for (BatchGetResponse.BatchGetSummary.GetSummary summary : summaries) {
      String key = summary.getKey();
      GetResponse getResponse = summary.getGetResponse().join();

      assertThat(key)
          .isIn(new String(key1, StandardCharsets.UTF_8), new String(key2, StandardCharsets.UTF_8));

      if (key.equals(new String(key1, StandardCharsets.UTF_8))) {
        validateResponse(getResponse, new String(value1, StandardCharsets.UTF_8));
      } else if (key.equals(new String(key2, StandardCharsets.UTF_8))) {
        validateResponse(getResponse, new String(value2, StandardCharsets.UTF_8));
      } else {
        fail("Unexpected key encountered in the response");
      }
    }
  }

  @Test
  void testBatchGetWithStringKeys_MoreThanMaxConcurrentThrows()
      throws ExecutionException, InterruptedException {
    MomentoBatchUtils momentoBatchUtils =
        MomentoBatchUtils.builder(cacheClient).withMaxConcurrentRequests(1).build();
    // Setup test data
    String key1 = "testKey1";
    String value1 = "testValue1";
    cacheClient.set(cacheName, key1, value1).join();

    String key2 = "testKey2";
    String value2 = "testValue2";
    cacheClient.set(cacheName, key2, value2).join();

    // Create a batch get request
    BatchGetRequest.StringKeyBatchGetRequest request =
        new BatchGetRequest.StringKeyBatchGetRequest(Arrays.asList(key1, key2));

    // Perform batch get
    BatchGetResponse response = momentoBatchUtils.batchGet(cacheName, request);

    // Assertions
    assertThat(response).isNotNull();
    assertThat(response).isInstanceOf(BatchGetResponse.Error.class);
    assertThat((((BatchGetResponse.Error) response)).getMessage())
        .isEqualTo(
            "Invalid argument passed to Momento client: Number of keys should be less than or equal to "
                + "maxConcurrentRequests. You can configure this value using MomentoBatchUtilsBuilder option "
                + "withMaxConcurrentRequests. Current value for maxConcurrentRequests: 1");
  }
}
