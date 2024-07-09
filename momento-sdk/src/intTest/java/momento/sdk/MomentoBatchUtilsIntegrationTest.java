package momento.sdk;

import static momento.sdk.TestUtils.randomBytes;
import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import momento.sdk.batchutils.MomentoBatchUtils;
import momento.sdk.batchutils.request.BatchGetRequest;
import momento.sdk.batchutils.response.BatchGetResponse;
import momento.sdk.responses.cache.GetResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MomentoBatchUtilsIntegrationTest extends BaseCacheTestClass {
  private static MomentoBatchUtils momentoBatchUtils;

  @BeforeAll
  static void setup() {
    momentoBatchUtils = MomentoBatchUtils.builder(cacheClient).build();
  }

  @AfterAll
  static void teardown() {
    momentoBatchUtils.close();
  }

  @Test
  void testBatchGetWithStringKeys() {
    // Setup test data
    String key1 = randomString();
    String value1 = randomString();
    cacheClient.set(cacheName, key1, value1).join();

    String key2 = randomString();
    String value2 = randomString();
    cacheClient.set(cacheName, key2, value2).join();

    // Create a batch get request
    BatchGetRequest.StringKeyBatchGetRequest request =
        new BatchGetRequest.StringKeyBatchGetRequest(Arrays.asList(key1, key2));

    // Perform batch get
    BatchGetResponse response = momentoBatchUtils.batchGet(cacheName, request).join();

    // Assertions
    assertThat(response).isNotNull();
    assertThat(response).isInstanceOf(BatchGetResponse.StringKeyBatchGetSummary.class);
    List<BatchGetResponse.StringKeyBatchGetSummary.GetSummary> summaries =
        ((BatchGetResponse.StringKeyBatchGetSummary) response).getSummaries();
    assertThat(summaries).hasSize(2);

    boolean key1Found = false;
    boolean key2Found = false;
    // Assert each response
    for (BatchGetResponse.StringKeyBatchGetSummary.GetSummary summary : summaries) {
      String key = summary.getKey();
      GetResponse getResponse = summary.getGetResponse();

      assertThat(key).isIn(key1, key2);

      if (key.equals(key1)) {
        key1Found = true;
        validateResponse(getResponse, value1);
      } else if (key.equals(key2)) {
        key2Found = true;
        validateResponse(getResponse, value2);
      } else {
        fail("Unexpected key encountered in the response");
      }
    }

    if (!key1Found || !key2Found) {
      fail("key not encountered in the response and should have been found");
    }
  }

  private void validateResponse(GetResponse getResponse, String expectedValue) {
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    GetResponse.Hit hitResponse = (GetResponse.Hit) getResponse;
    String retrievedValue = hitResponse.valueString();
    assertThat(retrievedValue).isEqualTo(expectedValue);
  }

  @Test
  void testBatchGetWithByteArrayKeys() {
    // Setup test data
    byte[] key1 = randomBytes();
    byte[] value1 = randomBytes();
    cacheClient.set(cacheName, key1, value1).join();

    byte[] key2 = randomBytes();
    byte[] value2 = randomBytes();
    cacheClient.set(cacheName, key2, value2).join();

    // Create a batch get request
    BatchGetRequest.ByteArrayKeyBatchGetRequest request =
        new BatchGetRequest.ByteArrayKeyBatchGetRequest(Arrays.asList(key1, key2));

    // Perform batch get
    BatchGetResponse response = momentoBatchUtils.batchGet(cacheName, request).join();

    // Assertions
    assertThat(response).isNotNull();
    assertThat(response).isInstanceOf(BatchGetResponse.ByteArrayKeyBatchGetSummary.class);
    List<BatchGetResponse.ByteArrayKeyBatchGetSummary.GetSummary> summaries =
        ((BatchGetResponse.ByteArrayKeyBatchGetSummary) response).getSummaries();
    assertThat(summaries).hasSize(2);

    boolean key1Found = false;
    boolean key2Found = false;
    // Assert each response
    for (BatchGetResponse.ByteArrayKeyBatchGetSummary.GetSummary summary : summaries) {
      byte[] key = summary.getKey();
      GetResponse getResponse = summary.getGetResponse();

      assertThat(key).isIn(key1, key2);

      if (Arrays.equals(key, key1)) {
        key1Found = true;
        validateResponse(getResponse, new String(value1, StandardCharsets.UTF_8));
      } else if (Arrays.equals(key, key2)) {
        key2Found = true;
        validateResponse(getResponse, new String(value2, StandardCharsets.UTF_8));
      } else {
        fail("Unexpected key encountered in the response");
      }
    }

    if (!key1Found || !key2Found) {
      fail("key not encountered in the response and should have been found");
    }
  }

  @Test
  void testBatchGetWithStringKeys_RequestsMoreThanMaxConn() {
    final int numberOfKeys = 10;
    final MomentoBatchUtils limitedBatchUtils =
        MomentoBatchUtils.builder(cacheClient)
            .withMaxConcurrentRequests(1)
            .withRequestTimeoutSeconds(5)
            .build();

    // test data with more keys than max concurrent requests
    ArrayList<String> keys = new ArrayList<>();
    ArrayList<String> values = new ArrayList<>();
    for (int i = 0; i < numberOfKeys; i++) {
      String key = randomString("testKey" + i);
      String value = randomString("testValue" + i);
      cacheClient.set(cacheName, key, value).join();

      keys.add(key);
      values.add(value);
    }

    BatchGetRequest.StringKeyBatchGetRequest request =
        new BatchGetRequest.StringKeyBatchGetRequest(keys);

    // Perform batch get
    BatchGetResponse response = limitedBatchUtils.batchGet(cacheName, request).join();

    // Assertions
    assertThat(response).isNotNull();
    assertThat(response).isInstanceOf(BatchGetResponse.StringKeyBatchGetSummary.class);
    List<BatchGetResponse.StringKeyBatchGetSummary.GetSummary> summaries =
        ((BatchGetResponse.StringKeyBatchGetSummary) response).getSummaries();
    assertThat(summaries).hasSize(numberOfKeys);

    // Assert each response
    for (int i = 0; i < numberOfKeys; i++) {
      String expectedKey = keys.get(i);
      String expectedValue = values.get(i);
      boolean keyFound =
          summaries.stream()
              .anyMatch(
                  summary ->
                      expectedKey.equals(summary.getKey())
                          && expectedValue.equals(
                              ((GetResponse.Hit) summary.getGetResponse()).valueString()));

      assertThat(keyFound)
          .withFailMessage("Key " + expectedKey + " not found in the response")
          .isTrue();
    }

    limitedBatchUtils.close();
  }

  @Test
  void testBatchGetOrderWithStringKeys() {
    // Setup test data with a specific order
    List<String> keys =
        Arrays.asList(randomString("key3"), randomString("key1"), randomString("key2"));
    List<String> values = Arrays.asList("value3", "value1", "value2");

    for (int i = 0; i < keys.size(); i++) {
      cacheClient.set(cacheName, keys.get(i), values.get(i)).join();
    }

    // Batch get request with keys in specific order
    BatchGetRequest.StringKeyBatchGetRequest request =
        new BatchGetRequest.StringKeyBatchGetRequest(keys);

    BatchGetResponse response = momentoBatchUtils.batchGet(cacheName, request).join();

    // Assertions
    assertThat(response).isNotNull();
    assertThat(response).isInstanceOf(BatchGetResponse.StringKeyBatchGetSummary.class);
    List<BatchGetResponse.StringKeyBatchGetSummary.GetSummary> summaries =
        ((BatchGetResponse.StringKeyBatchGetSummary) response).getSummaries();

    assertThat(summaries).hasSize(keys.size());

    // Assert that the order of keys in the response matches the order of requested keys
    for (int i = 0; i < keys.size(); i++) {
      assertThat(summaries.get(i).getKey()).isEqualTo(keys.get(i));
      validateResponse(summaries.get(i).getGetResponse(), values.get(i));
    }
  }
}
