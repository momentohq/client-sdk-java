package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import momento.sdk.messages.CacheListConcatenateBackResponse;
import momento.sdk.messages.CacheListFetchResponse;
import momento.sdk.requests.CollectionTtl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListTest extends BaseTestClass {
  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  private CacheClient target;
  private String cacheName;

  private final List<String> values = Arrays.asList("val1", "val2", "val3", "val4");

  private final String listName = "listName";

  @BeforeEach
  void setup() {
    target = CacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), DEFAULT_TTL_SECONDS).build();
    cacheName = System.getenv("TEST_CACHE_NAME");
    target.createCache(cacheName);
  }

  @AfterEach
  void teardown() {
    target.deleteCache(cacheName);
    target.close();
  }

  @Test
  public void shouldAddStringValueToBackOfListWhenListConcatenateBack() {
    List<String> oldValues = Arrays.asList("val1", "val2", "val3");
    List<String> newValues = Arrays.asList("val4", "val5", "val6");
    CacheListConcatenateBackResponse cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(
                cacheName, listName, oldValues, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);
    assertThat(
            ((CacheListConcatenateBackResponse.Success) cacheListConcatenateBackResponse)
                .getListLength())
        .isEqualTo(oldValues.size());

    cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(
                cacheName, listName, newValues, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);
    assertThat(
            ((CacheListConcatenateBackResponse.Success) cacheListConcatenateBackResponse)
                .getListLength())
        .isEqualTo(oldValues.size() + newValues.size());
  }

  @Test
  public void shouldAddByteValueToBackOfListWhenListConcatenateBack() {
    final String listName = "listName";
    final List<byte[]> oldValues = Arrays.asList("val1".getBytes(), "val2".getBytes());
    final List<byte[]> newValues = Arrays.asList("val3".getBytes(), "val4".getBytes());
    CacheListConcatenateBackResponse cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(
                cacheName, listName, oldValues, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);
    assertThat(
            ((CacheListConcatenateBackResponse.Success) cacheListConcatenateBackResponse)
                .getListLength())
        .isEqualTo(oldValues.size());

    cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(
                cacheName, listName, newValues, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);
    assertThat(
            ((CacheListConcatenateBackResponse.Success) cacheListConcatenateBackResponse)
                .getListLength())
        .isEqualTo(oldValues.size() + newValues.size());
  }

  @Test
  public void shouldFailListConcatenateBackWhenNullCacheName() {
    CacheListConcatenateBackResponse cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(null, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Error.class);
  }

  @Test
  public void shouldFailListConcatenateBackWhenNullListName() {
    CacheListConcatenateBackResponse cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(cacheName, null, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Error.class);
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithPositiveStartEndIndices() {
    final String listName = "listName";
    target
        .listConcatenateBack(cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
        .join();

    CacheListFetchResponse cacheListFetchResponse =
        target.listFetch(cacheName, listName, 1, 3).join();

    assertThat(cacheListFetchResponse).isInstanceOf(CacheListFetchResponse.Hit.class);

    List<String> expectedResult = Arrays.asList("val2", "val3");
    assertThat(((CacheListFetchResponse.Hit) cacheListFetchResponse).valueListString())
        .isEqualTo(expectedResult);
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithNegativeStartEndIndices() {
    final String listName = "listName";
    target
        .listConcatenateBack(cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
        .join();

    CacheListFetchResponse cacheListFetchResponse =
        target.listFetch(cacheName, listName, -3, -1).join();

    assertThat(cacheListFetchResponse).isInstanceOf(CacheListFetchResponse.Hit.class);

    List<String> expectedResult = Arrays.asList("val2", "val3");
    assertThat(((CacheListFetchResponse.Hit) cacheListFetchResponse).valueListString())
        .isEqualTo(expectedResult);
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithNullStartIndex() {
    final String listName = "listName";
    target
        .listConcatenateBack(cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
        .join();

    // valid case for null startIndex and positive endIndex
    CacheListFetchResponse cacheListFetchResponse =
        target.listFetch(cacheName, listName, null, 1).join();

    assertThat(cacheListFetchResponse).isInstanceOf(CacheListFetchResponse.Hit.class);

    List<String> expectedResult = Collections.singletonList("val1");
    assertThat(((CacheListFetchResponse.Hit) cacheListFetchResponse).valueListString())
        .isEqualTo(expectedResult);

    // valid case for null startIndex and negative endIndex
    cacheListFetchResponse = target.listFetch(cacheName, listName, null, -3).join();

    assertThat(cacheListFetchResponse).isInstanceOf(CacheListFetchResponse.Hit.class);

    expectedResult = Collections.singletonList("val1");
    assertThat(((CacheListFetchResponse.Hit) cacheListFetchResponse).valueListString())
        .isEqualTo(expectedResult);
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithNullEndIndex() {
    final String listName = "listName";
    target
        .listConcatenateBack(cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
        .join();

    // valid case for positive startIndex and null endIndex
    CacheListFetchResponse cacheListFetchResponse =
        target.listFetch(cacheName, listName, 2, null).join();

    assertThat(cacheListFetchResponse).isInstanceOf(CacheListFetchResponse.Hit.class);

    List<String> expectedResult = Arrays.asList("val3", "val4");
    assertThat(((CacheListFetchResponse.Hit) cacheListFetchResponse).valueListString())
        .isEqualTo(expectedResult);

    // valid case for negative startIndex and null endIndex
    cacheListFetchResponse = target.listFetch(cacheName, listName, -3, null).join();

    assertThat(cacheListFetchResponse).isInstanceOf(CacheListFetchResponse.Hit.class);

    expectedResult = Arrays.asList("val2", "val3", "val4");
    assertThat(((CacheListFetchResponse.Hit) cacheListFetchResponse).valueListString())
        .isEqualTo(expectedResult);
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithInvalidIndices() {
    final String listName = "listName";
    target
        .listConcatenateBack(cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
        .join();

    // the positive startIndex is larger than the positive endIndex
    CacheListFetchResponse cacheListFetchResponse =
        target.listFetch(cacheName, listName, 3, 1).join();

    assertThat(cacheListFetchResponse).isInstanceOf(CacheListFetchResponse.Error.class);

    // the positive startIndex is the same value as the positive endIndex
    cacheListFetchResponse = target.listFetch(cacheName, listName, 3, 3).join();

    assertThat(cacheListFetchResponse).isInstanceOf(CacheListFetchResponse.Error.class);

    // the negative startIndex is larger than the negative endIndex
    cacheListFetchResponse = target.listFetch(cacheName, listName, -2, -3).join();

    assertThat(cacheListFetchResponse).isInstanceOf(CacheListFetchResponse.Error.class);
  }
}
