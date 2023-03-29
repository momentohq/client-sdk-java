package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.messages.CacheListConcatenateBackResponse;
import momento.sdk.messages.CacheListConcatenateFrontResponse;
import momento.sdk.messages.CacheListFetchResponse;
import momento.sdk.messages.CacheListLengthResponse;
import momento.sdk.messages.CacheListPopBackResponse;
import momento.sdk.messages.CacheListPopFrontResponse;
import momento.sdk.requests.CollectionTtl;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListTest extends BaseTestClass {
  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);

  private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
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
  public void listConcatenateBackStringHappyPath() {
    List<String> oldValues = Arrays.asList("val1", "val2", "val3");
    List<String> newValues = Arrays.asList("val4", "val5", "val6");

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateBackString(
                cacheName, listName, oldValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(3).containsAll(oldValues));

    // Add same list
    assertThat(
            target.listConcatenateBackString(
                cacheName, listName, oldValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    List<String> expectedList = Arrays.asList("val1", "val2", "val3", "val1", "val2", "val3");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(6).containsAll(expectedList));

    // Add a new values list
    assertThat(
            target.listConcatenateBackString(
                cacheName, listName, newValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    List<String> newExpectedList =
        Arrays.asList("val1", "val2", "val3", "val1", "val2", "val3", "val4", "val5", "val6");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListString()).hasSize(9).containsAll(newExpectedList));
  }

  @Test
  public void listConcatenateBackByteArrayHappyPath() {
    List<byte[]> oldValues = Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());
    List<byte[]> newValues = Arrays.asList("val4".getBytes(), "val5".getBytes(), "val6".getBytes());

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateBackByteArray(
                cacheName, listName, oldValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListByteArray()).hasSize(3).containsAll(oldValues));

    // Add same list
    assertThat(
            target.listConcatenateBackByteArray(
                cacheName, listName, oldValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    List<byte[]> expectedList =
        Arrays.asList(
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes(),
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes());
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListByteArray()).hasSize(6).containsAll(expectedList));

    // Add a new values list
    assertThat(
            target.listConcatenateBackByteArray(
                cacheName, listName, newValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    List<byte[]> newExpectedList =
        Arrays.asList(
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes(),
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes(),
            "val4".getBytes(),
            "val5".getBytes(),
            "val6".getBytes());
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListByteArray()).hasSize(9).containsAll(newExpectedList));
  }

  @Test
  public void shouldFailListConcatenateBackWhenNullCacheName() {
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    assertThat(
            target.listConcatenateBackString(
                null, listName, stringValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            target.listConcatenateBackByteArray(
                null, listName, byteArrayValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldFailListConcatenateBackWhenNullListName() {
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    assertThat(
            target.listConcatenateBackString(
                cacheName, null, stringValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            target.listConcatenateBackByteArray(
                cacheName, null, byteArrayValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithPositiveStartEndIndices() {
    final String listName = "listName";
    target
        .listConcatenateBackString(
            cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
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
        .listConcatenateBackString(
            cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
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
        .listConcatenateBackString(
            cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
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
        .listConcatenateBackString(
            cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
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
        .listConcatenateBackString(
            cacheName, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
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

  @Test
  public void listConcatenateFrontStringHappyPath() {
    List<String> oldValues = Arrays.asList("val1", "val2", "val3");
    List<String> newValues = Arrays.asList("val4", "val5", "val6");

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateFrontString(
                cacheName, listName, oldValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(3).containsAll(oldValues));

    // Add same list
    assertThat(
            target.listConcatenateFrontString(
                cacheName, listName, oldValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    List<String> expectedList = Arrays.asList("val1", "val2", "val3", "val1", "val2", "val3");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(6).containsAll(expectedList));

    // Add a new values list
    assertThat(
            target.listConcatenateFrontString(
                cacheName, listName, newValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    List<String> newExpectedList =
        Arrays.asList("val4", "val5", "val6", "val1", "val2", "val3", "val1", "val2", "val3");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListString()).hasSize(9).containsAll(newExpectedList));
  }

  @Test
  public void listConcatenateFrontByteArrayHappyPath() {
    List<byte[]> oldValues = Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());
    List<byte[]> newValues = Arrays.asList("val4".getBytes(), "val5".getBytes(), "val6".getBytes());

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, listName, oldValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListByteArray()).hasSize(3).containsAll(oldValues));

    // Add same list
    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, listName, oldValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    List<byte[]> expectedList =
        Arrays.asList(
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes(),
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes());
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListByteArray()).hasSize(6).containsAll(expectedList));

    // Add a new values list
    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, listName, newValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    List<byte[]> newExpectedList =
        Arrays.asList(
            "val4".getBytes(),
            "val5".getBytes(),
            "val6".getBytes(),
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes(),
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes());
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListByteArray()).hasSize(9).containsAll(newExpectedList));
  }

  @Test
  public void shouldFailListConcatenateFrontWhenNullCacheName() {
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    assertThat(
            target.listConcatenateFrontString(
                null, listName, stringValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            target.listConcatenateFrontByteArray(
                null, listName, byteArrayValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldFailListConcatenateFrontWhenNullListName() {
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    assertThat(
            target.listConcatenateFrontString(
                cacheName, null, stringValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, null, byteArrayValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listLengthHappyPath() {
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    assertThat(target.listLength(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListLengthResponse.Miss.class);

    // add string values to list
    assertThat(
            target.listConcatenateFrontString(
                cacheName, listName, stringValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listLength(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListLengthResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.getListLength()).isEqualTo(stringValues.size()));

    // add byte array values to list
    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, listName, byteArrayValues, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listLength(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListLengthResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.getListLength())
                    .isEqualTo(stringValues.size() + byteArrayValues.size()));
  }

  @Test
  public void shouldFailListLengthWhenNullCacheName() {
    assertThat(target.listLength(null, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListLengthResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldFailListLengthWhenNullListName() {
    assertThat(target.listLength(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListLengthResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listPopBackHappyPath() {
    List<String> values = Arrays.asList("val1", "val2", "val3");

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateBackString(
                cacheName, listName, values, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    // Pop the value as string from back of the list
    assertThat(target.listPopBack(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopBackResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueString()).isEqualTo("val3"));

    // Pop the value as byte array from the back of the new list
    assertThat(target.listPopBack(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopBackResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueByteArray()).isEqualTo("val2".getBytes()));
  }

  @Test
  public void shouldFailListPopBackWhenNullCacheName() {
    assertThat(target.listPopBack(null, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldFailListPopBackWhenNullListName() {
    assertThat(target.listPopBack(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listPopFrontHappyPath() {
    List<String> values = Arrays.asList("val1", "val2", "val3");

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateBackString(
                cacheName, listName, values, CollectionTtl.fromCacheTtl(), 0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    // Pop the value as string from back of the list
    assertThat(target.listPopFront(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopFrontResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueString()).isEqualTo("val1"));

    // Pop the value as byte array from the back of the new list
    assertThat(target.listPopFront(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopFrontResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueByteArray()).isEqualTo("val2".getBytes()));
  }

  @Test
  public void shouldFailListPopFrontWhenNullCacheName() {
    assertThat(target.listPopFront(null, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldFailListPopFrontWhenNullListName() {
    assertThat(target.listPopFront(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }
}
