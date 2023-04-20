package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.messages.CacheListConcatenateBackResponse;
import momento.sdk.messages.CacheListConcatenateFrontResponse;
import momento.sdk.messages.CacheListFetchResponse;
import momento.sdk.messages.CacheListLengthResponse;
import momento.sdk.messages.CacheListPopBackResponse;
import momento.sdk.messages.CacheListPopFrontResponse;
import momento.sdk.messages.CacheListPushBackResponse;
import momento.sdk.messages.CacheListPushFrontResponse;
import momento.sdk.messages.CacheListRemoveValueResponse;
import momento.sdk.messages.CacheListRetainResponse;
import momento.sdk.requests.CollectionTtl;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListTest extends BaseTestClass {
  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);

  private CacheClient target;

  private final String cacheName = System.getenv("TEST_CACHE_NAME");

  private final List<String> values = Arrays.asList("val1", "val2", "val3", "val4");

  private final String listName = "listName";

  @BeforeEach
  void setup() {
    target =
        CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
            .build();
    target.createCache(cacheName).join();
  }

  @AfterEach
  void teardown() {
    target.deleteCache(cacheName).join();
    target.close();
  }

  @Test
  public void listConcatenateBackStringHappyPath() {
    final List<String> oldValues = Arrays.asList("val1", "val2", "val3");
    final List<String> newValues = Arrays.asList("val4", "val5", "val6");

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateBack(
                cacheName, listName, oldValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString()).hasSize(3).containsExactlyElementsOf(oldValues));

    assertThat(target.listConcatenateBack(cacheName, listName, newValues))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    final Iterable<String> expectedList = Iterables.concat(oldValues, newValues);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(6)
                    .containsExactlyElementsOf(expectedList));

    // Add the original values again and truncate the list to 6 items
    assertThat(target.listConcatenateBack(cacheName, listName, oldValues, 6))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    final Iterable<String> newExpectedList = Iterables.concat(newValues, oldValues);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(6)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @Test
  public void listConcatenateBackByteArrayHappyPath() {
    final List<byte[]> oldValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());
    final List<byte[]> newValues =
        Arrays.asList("val4".getBytes(), "val5".getBytes(), "val6".getBytes());

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateBackByteArray(
                cacheName, listName, oldValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(3)
                    .containsExactlyElementsOf(oldValues));

    assertThat(target.listConcatenateBackByteArray(cacheName, listName, newValues))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    final Iterable<byte[]> expectedList = Iterables.concat(oldValues, newValues);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(6)
                    .containsExactlyElementsOf(expectedList));

    // Add the original values again and truncate the list to 6 items
    assertThat(target.listConcatenateBackByteArray(cacheName, listName, oldValues, 6))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    final Iterable<byte[]> newExpectedList = Iterables.concat(newValues, oldValues);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(6)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateBackWhenNullCacheName() {
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateBack(
                null, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateBack(null, listName, stringValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(target.listConcatenateBack(null, listName, stringValues))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateBackByteArray(
                null, listName, byteArrayValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateBackByteArray(null, listName, byteArrayValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(target.listConcatenateBackByteArray(null, listName, byteArrayValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateBackWhenNullListName() {
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateBack(
                cacheName, null, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateBack(cacheName, null, stringValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(target.listConcatenateBack(cacheName, null, stringValues))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateBackByteArray(
                cacheName, null, byteArrayValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateBackByteArray(cacheName, null, byteArrayValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(target.listConcatenateBackByteArray(cacheName, null, byteArrayValues))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateBackWhenNullElement() {
    // With ttl specified in method signature
    assertThat(
            target.listConcatenateBack(
                cacheName, listName, null, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateBack(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(target.listConcatenateBack(cacheName, listName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateBackByteArray(
                cacheName, listName, null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateBackByteArray(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(target.listConcatenateBackByteArray(cacheName, listName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithPositiveStartEndIndices() {
    final String listName = "listName";
    target
        .listConcatenateBack(
            cacheName, listName, values, null, CollectionTtl.of(DEFAULT_TTL_SECONDS))
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
        .listConcatenateBack(
            cacheName, listName, values, null, CollectionTtl.of(DEFAULT_TTL_SECONDS))
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
        .listConcatenateBack(
            cacheName, listName, values, null, CollectionTtl.of(DEFAULT_TTL_SECONDS))
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
        .listConcatenateBack(
            cacheName, listName, values, null, CollectionTtl.of(DEFAULT_TTL_SECONDS))
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
        .listConcatenateBack(cacheName, listName, values, 0, CollectionTtl.of(DEFAULT_TTL_SECONDS))
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
    final List<String> oldValues = Arrays.asList("val1", "val2", "val3");
    final List<String> newValues = Arrays.asList("val4", "val5", "val6");

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateFront(
                cacheName, listName, oldValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString()).hasSize(3).containsExactlyElementsOf(oldValues));

    assertThat(target.listConcatenateFront(cacheName, listName, newValues))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    final Iterable<String> expectedList = Iterables.concat(newValues, oldValues);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(6)
                    .containsExactlyElementsOf(expectedList));

    // Add the original values again and truncate the list to 6 items
    assertThat(target.listConcatenateFront(cacheName, listName, oldValues, 6))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    final Iterable<String> newExpectedList = Iterables.concat(oldValues, newValues);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(6)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @Test
  public void listConcatenateFrontByteArrayHappyPath() {
    final List<byte[]> oldValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());
    final List<byte[]> newValues =
        Arrays.asList("val4".getBytes(), "val5".getBytes(), "val6".getBytes());

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, listName, oldValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(3)
                    .containsExactlyElementsOf(oldValues));

    assertThat(target.listConcatenateFrontByteArray(cacheName, listName, newValues))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    final Iterable<byte[]> expectedList = Iterables.concat(newValues, oldValues);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(6)
                    .containsExactlyElementsOf(expectedList));

    // Add the original values again and truncate the list to 6 items
    assertThat(target.listConcatenateFrontByteArray(cacheName, listName, oldValues, 6))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    final Iterable<byte[]> newExpectedList = Iterables.concat(oldValues, newValues);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(6)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateFrontWhenNullCacheName() {
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateFront(
                null, listName, stringValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateFront(null, listName, stringValues, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateFrontByteArray(
                null, listName, byteArrayValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(
            target.listConcatenateFrontByteArray(
                null, listName, byteArrayValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateFrontWhenNullListName() {
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateFront(
                cacheName, null, stringValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateFront(cacheName, null, stringValues, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, null, byteArrayValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateFrontByteArray(cacheName, null, byteArrayValues, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateFrontWhenNullElement() {
    // With ttl specified in method signature
    assertThat(
            target.listConcatenateFront(cacheName, listName, null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateFront(cacheName, listName, null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, listName, null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listConcatenateFrontByteArray(cacheName, listName, null, 0))
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
            target.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listLength(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListLengthResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.getListLength()).isEqualTo(stringValues.size()));

    // add byte array values to list
    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, listName, byteArrayValues, null, CollectionTtl.fromCacheTtl()))
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

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListLengthWhenNullCacheName() {
    assertThat(target.listLength(null, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListLengthResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
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
            target.listConcatenateBack(
                cacheName, listName, values, null, CollectionTtl.fromCacheTtl()))
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

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPopBackWhenNullCacheName() {
    assertThat(target.listPopBack(null, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
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
            target.listConcatenateBack(
                cacheName, listName, values, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);

    // Pop the value as string from front of the list
    assertThat(target.listPopFront(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopFrontResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueString()).isEqualTo("val1"));

    // Pop the value as byte array from the front of the new list
    assertThat(target.listPopFront(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopFrontResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueByteArray()).isEqualTo("val2".getBytes()));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPopFrontWhenNullCacheName() {
    assertThat(target.listPopFront(null, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPopFrontWhenNullListName() {
    assertThat(target.listPopFront(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPopFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listPushBackStringHappyPath() {
    final String oldValue = "val1";
    final String newValue = "val2";

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listPushBack(cacheName, listName, oldValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushBackResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(1).containsOnly(oldValue));

    // Add the same value
    assertThat(target.listPushBack(cacheName, listName, oldValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushBackResponse.Success.class);

    final List<String> expectedList = Arrays.asList(oldValue, oldValue);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(2)
                    .containsExactlyElementsOf(expectedList));

    // Add a new value and truncate the list to 2 items
    assertThat(target.listPushBack(cacheName, listName, newValue, 2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushBackResponse.Success.class);

    final List<String> newExpectedList = Arrays.asList(oldValue, newValue);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(2)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @Test
  public void listPushBackByteArrayHappyPath() {
    final byte[] oldValue = "val1".getBytes();
    final byte[] newValue = "val2".getBytes();

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listPushBack(cacheName, listName, oldValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushBackResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListByteArray()).hasSize(1).containsOnly(oldValue));

    // Add the same value
    assertThat(target.listPushBack(cacheName, listName, oldValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushBackResponse.Success.class);

    final List<byte[]> expectedList = Arrays.asList(oldValue, oldValue);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(2)
                    .containsExactlyElementsOf(expectedList));

    // Add a new value and truncate the list to 2 items
    assertThat(target.listPushBack(cacheName, listName, newValue, 2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushBackResponse.Success.class);

    final List<byte[]> newExpectedList = Arrays.asList(oldValue, newValue);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(2)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushBackWhenNullCacheName() {
    final String stringValue = "val1";
    final byte[] byteArrayValue = "val1".getBytes();

    // With ttl specified in method signature
    assertThat(target.listPushBack(null, listName, stringValue, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushBack(null, listName, stringValue, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(target.listPushBack(null, listName, byteArrayValue, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushBack(null, listName, byteArrayValue, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushBackWhenNullListName() {
    final String stringValue = "val1";
    final byte[] byteArrayValue = "val1".getBytes();

    // With ttl specified in method signature
    assertThat(target.listPushBack(cacheName, null, stringValue, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushBack(cacheName, null, stringValue, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listPushBack(cacheName, null, byteArrayValue, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushBack(cacheName, null, byteArrayValue, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushBackWhenNullElement() {
    // With ttl specified in method signature
    assertThat(
            target.listPushBack(
                cacheName, listName, (String) null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushBack(cacheName, listName, (String) null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listPushBack(
                cacheName, listName, (byte[]) null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushBack(cacheName, listName, (byte[]) null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listPushFrontStringHappyPath() {
    final String oldValue = "val1";
    final String newValue = "val2";

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listPushFront(cacheName, listName, oldValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(1).containsOnly(oldValue));

    // Add the same value
    assertThat(target.listPushFront(cacheName, listName, oldValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushFrontResponse.Success.class);

    final List<String> expectedList = Arrays.asList(oldValue, oldValue);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(2)
                    .containsExactlyElementsOf(expectedList));

    // Add a new value and truncate the list to 2 items
    assertThat(target.listPushFront(cacheName, listName, newValue, 2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushFrontResponse.Success.class);

    final List<String> newExpectedList = Arrays.asList(newValue, oldValue);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(2)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @Test
  public void listPushFrontByteArrayHappyPath() {
    final byte[] oldValue = "val1".getBytes();
    final byte[] newValue = "val2".getBytes();

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListFetchResponse.Miss.class);

    assertThat(
            target.listPushFront(cacheName, listName, oldValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListByteArray()).hasSize(1).containsOnly(oldValue));

    // Add the same value
    assertThat(target.listPushFront(cacheName, listName, oldValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushFrontResponse.Success.class);

    final List<byte[]> expectedList = Arrays.asList(oldValue, oldValue);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(2)
                    .containsExactlyElementsOf(expectedList));

    // Add a new value and truncate the list to 2 items
    assertThat(target.listPushFront(cacheName, listName, newValue, 2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListPushFrontResponse.Success.class);

    final List<byte[]> newExpectedList = Arrays.asList(newValue, oldValue);
    assertThat(target.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(2)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushFrontWhenNullCacheName() {
    final String stringValue = "val1";
    final byte[] byteArrayValue = "val1".getBytes();

    // With ttl specified in method signature
    assertThat(
            target.listPushFront(null, listName, stringValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushFront(null, listName, stringValue, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listPushFront(
                null, listName, byteArrayValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushFront(null, listName, byteArrayValue, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushFrontWhenNullListName() {
    final String stringValue = "val1";
    final byte[] byteArrayValue = "val1".getBytes();

    // With ttl specified in method signature
    assertThat(
            target.listPushFront(cacheName, null, stringValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushFront(cacheName, null, stringValue, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listPushFront(
                cacheName, null, byteArrayValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushFront(cacheName, null, byteArrayValue, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushFrontWhenNullElement() {
    // With ttl specified in method signature
    assertThat(
            target.listPushFront(
                cacheName, listName, (String) null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushFront(cacheName, listName, (String) null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            target.listPushFront(
                cacheName, listName, (byte[]) null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(target.listPushFront(cacheName, listName, (byte[]) null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listRemoveValueStringHappyPath() {
    List<String> values = Arrays.asList("val1", "val1", "val2", "val3", "val4");

    assertThat(
            target.listConcatenateFront(
                cacheName, listName, values, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    // Remove value from list
    String removeValue = "val1";
    assertThat(target.listRemoveValue(cacheName, listName, removeValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListRemoveValueResponse.Success.class);

    List<String> expectedList = Arrays.asList("val2", "val3", "val4");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(3).containsAll(expectedList));
  }

  @Test
  public void listRemoveValueByteArrayHappyPath() {
    List<byte[]> values =
        Arrays.asList(
            "val1".getBytes(),
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes(),
            "val4".getBytes());

    assertThat(
            target.listConcatenateFrontByteArray(
                cacheName, listName, values, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    // Remove value from list
    byte[] removeValue = "val1".getBytes();
    assertThat(target.listRemoveValue(cacheName, listName, removeValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListRemoveValueResponse.Success.class);

    List<byte[]> expectedList =
        Arrays.asList("val2".getBytes(), "val3".getBytes(), "val4".getBytes());
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListByteArray()).hasSize(3).containsAll(expectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListRemoveValueWhenNullCacheName() {
    String stringValue = "val1";
    byte[] byteArrayValue = "val1".getBytes();

    assertThat(target.listRemoveValue(null, listName, stringValue))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(target.listRemoveValue(null, listName, byteArrayValue))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListRemoveValueWhenNullListName() {
    String stringValue = "val1";
    byte[] byteArrayValue = "val1".getBytes();

    assertThat(target.listRemoveValue(cacheName, null, stringValue))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(target.listRemoveValue(cacheName, null, byteArrayValue))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListRemoveValueWhenNullElement() {
    assertThat(target.listRemoveValue(cacheName, listName, (String) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(target.listRemoveValue(null, listName, (byte[]) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithPositiveStartEndIndices() {
    final String listName = "listName";
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3", "val4");

    assertThat(
            target.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(4).containsAll(stringValues));

    assertThat(target.listRetain(cacheName, listName, 1, 3))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListRetainResponse.Success.class);

    List<String> expectedList = Arrays.asList("val2", "val3");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(2).containsAll(expectedList));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithNegativeStartEndIndices() {
    final String listName = "listName";
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3", "val4");

    assertThat(
            target.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(4).containsAll(stringValues));

    assertThat(target.listRetain(cacheName, listName, -3, -1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListRetainResponse.Success.class);

    List<String> expectedList = Arrays.asList("val2", "val3");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(2).containsAll(expectedList));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithNullStartIndex() {
    final String listName = "listName";
    final List<String> stringValues =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8");

    assertThat(
            target.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));

    // valid case for null startIndex and positive endIndex
    assertThat(target.listRetain(cacheName, listName, null, 7))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListRetainResponse.Success.class);

    List<String> expectedList =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(7).containsAll(expectedList));

    // valid case for null startIndex and negative endIndex
    assertThat(target.listRetain(cacheName, listName, null, -3))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListRetainResponse.Success.class);

    List<String> newExpectedList = Arrays.asList("val1", "val2", "val3", "val4");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListString()).hasSize(4).containsAll(newExpectedList));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithNullEndIndex() {
    final String listName = "listName";
    final List<String> stringValues =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8");

    assertThat(
            target.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));

    // valid case for positive startIndex and null endIndex
    assertThat(target.listRetain(cacheName, listName, 2, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListRetainResponse.Success.class);

    List<String> expectedList = Arrays.asList("val3", "val4", "val5", "val6", "val7", "val8");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(6).containsAll(expectedList));

    // valid case for negative startIndex and null endIndex
    assertThat(target.listRetain(cacheName, listName, -4, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListRetainResponse.Success.class);

    List<String> newExpectedList = Arrays.asList("val5", "val6", "val7", "val8");
    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListString()).hasSize(4).containsAll(newExpectedList));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithNullStartAndEndIndices() {
    final String listName = "listName";
    final List<String> stringValues =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8");

    assertThat(
            target.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));

    // valid case for null startIndex and null endIndex
    assertThat(target.listRetain(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListRetainResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldRetainAllValuesWhenListRetainWithInvalidIndices() {
    final String listName = "listName";
    final List<String> stringValues =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8");

    assertThat(
            target.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheListConcatenateFrontResponse.Success.class);

    assertThat(target.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));

    // the positive startIndex is larger than the positive endIndex
    assertThat(target.listRetain(null, listName, 3, 1))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListRetainResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // the positive startIndex is the same value as the positive endIndex
    assertThat(target.listRetain(null, listName, 3, 3))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListRetainResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // the negative startIndex is the larger than the negative endIndex
    assertThat(target.listRetain(null, listName, -3, -5))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheListRetainResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }
}
