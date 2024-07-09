package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.requests.CollectionTtl;
import momento.sdk.responses.cache.list.ListConcatenateBackResponse;
import momento.sdk.responses.cache.list.ListConcatenateFrontResponse;
import momento.sdk.responses.cache.list.ListFetchResponse;
import momento.sdk.responses.cache.list.ListLengthResponse;
import momento.sdk.responses.cache.list.ListPopBackResponse;
import momento.sdk.responses.cache.list.ListPopFrontResponse;
import momento.sdk.responses.cache.list.ListPushBackResponse;
import momento.sdk.responses.cache.list.ListPushFrontResponse;
import momento.sdk.responses.cache.list.ListRemoveValueResponse;
import momento.sdk.responses.cache.list.ListRetainResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class ListTest extends BaseCacheTestClass {
  private final List<String> values = Arrays.asList("val1", "val2", "val3", "val4");

  @Test
  public void listConcatenateBackStringHappyPath() {
    final String listName = randomString();
    final List<String> oldValues = Arrays.asList("val1", "val2", "val3");
    final List<String> newValues = Arrays.asList("val4", "val5", "val6");

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listConcatenateBack(
                cacheName, listName, oldValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateBackResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString()).hasSize(3).containsExactlyElementsOf(oldValues));

    assertThat(cacheClient.listConcatenateBack(cacheName, listName, newValues))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateBackResponse.Success.class);

    final Iterable<String> expectedList = Iterables.concat(oldValues, newValues);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(6)
                    .containsExactlyElementsOf(expectedList));

    // Add the original values again and truncate the list to 6 items
    assertThat(cacheClient.listConcatenateBack(cacheName, listName, oldValues, 6))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateBackResponse.Success.class);

    final Iterable<String> newExpectedList = Iterables.concat(newValues, oldValues);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(6)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @Test
  public void listConcatenateBackByteArrayHappyPath() {
    final String listName = randomString();
    final List<byte[]> oldValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());
    final List<byte[]> newValues =
        Arrays.asList("val4".getBytes(), "val5".getBytes(), "val6".getBytes());

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listConcatenateBackByteArray(
                cacheName, listName, oldValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateBackResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(3)
                    .containsExactlyElementsOf(oldValues));

    assertThat(cacheClient.listConcatenateBackByteArray(cacheName, listName, newValues))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateBackResponse.Success.class);

    final Iterable<byte[]> expectedList = Iterables.concat(oldValues, newValues);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(6)
                    .containsExactlyElementsOf(expectedList));

    // Add the original values again and truncate the list to 6 items
    assertThat(cacheClient.listConcatenateBackByteArray(cacheName, listName, oldValues, 6))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateBackResponse.Success.class);

    final Iterable<byte[]> newExpectedList = Iterables.concat(newValues, oldValues);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(6)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateBackWhenNullCacheName() {
    final String listName = randomString();
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateBack(
                null, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateBack(null, listName, stringValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(cacheClient.listConcatenateBack(null, listName, stringValues))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateBackByteArray(
                null, listName, byteArrayValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateBackByteArray(null, listName, byteArrayValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(cacheClient.listConcatenateBackByteArray(null, listName, byteArrayValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
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
            cacheClient.listConcatenateBack(
                cacheName, null, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateBack(cacheName, null, stringValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(cacheClient.listConcatenateBack(cacheName, null, stringValues))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateBackByteArray(
                cacheName, null, byteArrayValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateBackByteArray(cacheName, null, byteArrayValues, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(cacheClient.listConcatenateBackByteArray(cacheName, null, byteArrayValues))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateBackWhenNullElement() {
    final String listName = randomString();
    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateBack(
                cacheName, listName, null, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateBack(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(cacheClient.listConcatenateBack(cacheName, listName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateBackByteArray(
                cacheName, listName, null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateBackByteArray(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl or truncate specified in method signature
    assertThat(cacheClient.listConcatenateBackByteArray(cacheName, listName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithPositiveStartEndIndices() {
    final String listName = randomString();
    cacheClient
        .listConcatenateBack(
            cacheName, listName, values, null, CollectionTtl.of(DEFAULT_TTL_SECONDS))
        .join();

    ListFetchResponse listFetchResponse = cacheClient.listFetch(cacheName, listName, 1, 3).join();

    assertThat(listFetchResponse).isInstanceOf(ListFetchResponse.Hit.class);

    List<String> expectedResult = Arrays.asList("val2", "val3");
    assertThat(((ListFetchResponse.Hit) listFetchResponse).valueListString())
        .isEqualTo(expectedResult);
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithNegativeStartEndIndices() {
    final String listName = randomString();
    cacheClient
        .listConcatenateBack(
            cacheName, listName, values, null, CollectionTtl.of(DEFAULT_TTL_SECONDS))
        .join();

    ListFetchResponse listFetchResponse = cacheClient.listFetch(cacheName, listName, -3, -1).join();

    assertThat(listFetchResponse).isInstanceOf(ListFetchResponse.Hit.class);

    List<String> expectedResult = Arrays.asList("val2", "val3");
    assertThat(((ListFetchResponse.Hit) listFetchResponse).valueListString())
        .isEqualTo(expectedResult);
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithNullStartIndex() {
    final String listName = randomString();
    cacheClient
        .listConcatenateBack(
            cacheName, listName, values, null, CollectionTtl.of(DEFAULT_TTL_SECONDS))
        .join();

    // valid case for null startIndex and positive endIndex
    ListFetchResponse listFetchResponse =
        cacheClient.listFetch(cacheName, listName, null, 1).join();

    assertThat(listFetchResponse).isInstanceOf(ListFetchResponse.Hit.class);

    List<String> expectedResult = Collections.singletonList("val1");
    assertThat(((ListFetchResponse.Hit) listFetchResponse).valueListString())
        .isEqualTo(expectedResult);

    // valid case for null startIndex and negative endIndex
    listFetchResponse = cacheClient.listFetch(cacheName, listName, null, -3).join();

    assertThat(listFetchResponse).isInstanceOf(ListFetchResponse.Hit.class);

    expectedResult = Collections.singletonList("val1");
    assertThat(((ListFetchResponse.Hit) listFetchResponse).valueListString())
        .isEqualTo(expectedResult);
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithNullEndIndex() {
    final String listName = randomString();
    cacheClient
        .listConcatenateBack(
            cacheName, listName, values, null, CollectionTtl.of(DEFAULT_TTL_SECONDS))
        .join();

    // valid case for positive startIndex and null endIndex
    ListFetchResponse listFetchResponse =
        cacheClient.listFetch(cacheName, listName, 2, null).join();

    assertThat(listFetchResponse).isInstanceOf(ListFetchResponse.Hit.class);

    List<String> expectedResult = Arrays.asList("val3", "val4");
    assertThat(((ListFetchResponse.Hit) listFetchResponse).valueListString())
        .isEqualTo(expectedResult);

    // valid case for negative startIndex and null endIndex
    listFetchResponse = cacheClient.listFetch(cacheName, listName, -3, null).join();

    assertThat(listFetchResponse).isInstanceOf(ListFetchResponse.Hit.class);

    expectedResult = Arrays.asList("val2", "val3", "val4");
    assertThat(((ListFetchResponse.Hit) listFetchResponse).valueListString())
        .isEqualTo(expectedResult);
  }

  @Test
  public void shouldFetchAllValuesWhenListFetchWithInvalidIndices() {
    final String listName = randomString();
    cacheClient
        .listConcatenateBack(cacheName, listName, values, 0, CollectionTtl.of(DEFAULT_TTL_SECONDS))
        .join();

    // the positive startIndex is larger than the positive endIndex
    ListFetchResponse listFetchResponse = cacheClient.listFetch(cacheName, listName, 3, 1).join();

    assertThat(listFetchResponse).isInstanceOf(ListFetchResponse.Error.class);

    // the positive startIndex is the same value as the positive endIndex
    listFetchResponse = cacheClient.listFetch(cacheName, listName, 3, 3).join();

    assertThat(listFetchResponse).isInstanceOf(ListFetchResponse.Error.class);

    // the negative startIndex is larger than the negative endIndex
    listFetchResponse = cacheClient.listFetch(cacheName, listName, -2, -3).join();

    assertThat(listFetchResponse).isInstanceOf(ListFetchResponse.Error.class);
  }

  @Test
  public void listConcatenateFrontStringHappyPath() {
    final String listName = randomString();
    final List<String> oldValues = Arrays.asList("val1", "val2", "val3");
    final List<String> newValues = Arrays.asList("val4", "val5", "val6");

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, oldValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString()).hasSize(3).containsExactlyElementsOf(oldValues));

    assertThat(cacheClient.listConcatenateFront(cacheName, listName, newValues))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    final Iterable<String> expectedList = Iterables.concat(newValues, oldValues);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(6)
                    .containsExactlyElementsOf(expectedList));

    // Add the original values again and truncate the list to 6 items
    assertThat(cacheClient.listConcatenateFront(cacheName, listName, oldValues, 6))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    final Iterable<String> newExpectedList = Iterables.concat(oldValues, newValues);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(6)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @Test
  public void listConcatenateFrontByteArrayHappyPath() {
    final String listName = randomString();
    final List<byte[]> oldValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());
    final List<byte[]> newValues =
        Arrays.asList("val4".getBytes(), "val5".getBytes(), "val6".getBytes());

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listConcatenateFrontByteArray(
                cacheName, listName, oldValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(3)
                    .containsExactlyElementsOf(oldValues));

    assertThat(cacheClient.listConcatenateFrontByteArray(cacheName, listName, newValues))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    final Iterable<byte[]> expectedList = Iterables.concat(newValues, oldValues);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(6)
                    .containsExactlyElementsOf(expectedList));

    // Add the original values again and truncate the list to 6 items
    assertThat(cacheClient.listConcatenateFrontByteArray(cacheName, listName, oldValues, 6))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    final Iterable<byte[]> newExpectedList = Iterables.concat(oldValues, newValues);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(6)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateFrontWhenNullCacheName() {
    final String listName = randomString();
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateFront(
                null, listName, stringValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateFront(null, listName, stringValues, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateFrontByteArray(
                null, listName, byteArrayValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateFrontByteArray(
                null, listName, byteArrayValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
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
            cacheClient.listConcatenateFront(
                cacheName, null, stringValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateFront(cacheName, null, stringValues, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateFrontByteArray(
                cacheName, null, byteArrayValues, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateFrontByteArray(cacheName, null, byteArrayValues, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListConcatenateFrontWhenNullElement() {
    final String listName = randomString();
    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateFront(cacheName, listName, null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listConcatenateFrontByteArray(
                cacheName, listName, null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listConcatenateFrontByteArray(cacheName, listName, null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListConcatenateFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listLengthHappyPath() {
    final String listName = randomString();
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3");
    final List<byte[]> byteArrayValues =
        Arrays.asList("val1".getBytes(), "val2".getBytes(), "val3".getBytes());

    assertThat(cacheClient.listLength(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListLengthResponse.Miss.class);

    // add string values to list
    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listLength(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListLengthResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.getListLength()).isEqualTo(stringValues.size()));

    // add byte array values to list
    assertThat(
            cacheClient.listConcatenateFrontByteArray(
                cacheName, listName, byteArrayValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listLength(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListLengthResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.getListLength())
                    .isEqualTo(stringValues.size() + byteArrayValues.size()));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListLengthWhenNullCacheName() {
    final String listName = randomString();
    assertThat(cacheClient.listLength(null, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListLengthResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListLengthWhenNullListName() {
    assertThat(cacheClient.listLength(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListLengthResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listPopBackHappyPath() {
    final String listName = randomString();
    List<String> values = Arrays.asList("val1", "val2", "val3");

    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listConcatenateBack(
                cacheName, listName, values, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateBackResponse.Success.class);

    // Pop the value as string from back of the list
    assertThat(cacheClient.listPopBack(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPopBackResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueString()).isEqualTo("val3"));

    // Pop the value as byte array from the back of the new list
    assertThat(cacheClient.listPopBack(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPopBackResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueByteArray()).isEqualTo("val2".getBytes()));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPopBackWhenNullCacheName() {
    final String listName = randomString();
    assertThat(cacheClient.listPopBack(null, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPopBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPopBackWhenNullListName() {
    assertThat(cacheClient.listPopBack(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPopBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listPopFrontHappyPath() {
    final String listName = randomString();
    List<String> values = Arrays.asList("val1", "val2", "val3");

    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listConcatenateBack(
                cacheName, listName, values, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateBackResponse.Success.class);

    // Pop the value as string from front of the list
    assertThat(cacheClient.listPopFront(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPopFrontResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueString()).isEqualTo("val1"));

    // Pop the value as byte array from the front of the new list
    assertThat(cacheClient.listPopFront(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPopFrontResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueByteArray()).isEqualTo("val2".getBytes()));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPopFrontWhenNullCacheName() {
    final String listName = randomString();
    assertThat(cacheClient.listPopFront(null, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPopFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPopFrontWhenNullListName() {
    assertThat(cacheClient.listPopFront(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPopFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listPushBackStringHappyPath() {
    final String listName = randomString();
    final String oldValue = "val1";
    final String newValue = "val2";

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listPushBack(
                cacheName, listName, oldValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushBackResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(1).containsOnly(oldValue));

    // Add the same value
    assertThat(cacheClient.listPushBack(cacheName, listName, oldValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushBackResponse.Success.class);

    final List<String> expectedList = Arrays.asList(oldValue, oldValue);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(2)
                    .containsExactlyElementsOf(expectedList));

    // Add a new value and truncate the list to 2 items
    assertThat(cacheClient.listPushBack(cacheName, listName, newValue, 2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushBackResponse.Success.class);

    final List<String> newExpectedList = Arrays.asList(oldValue, newValue);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(2)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @Test
  public void listPushBackByteArrayHappyPath() {
    final String listName = randomString();
    final byte[] oldValue = "val1".getBytes();
    final byte[] newValue = "val2".getBytes();

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listPushBack(
                cacheName, listName, oldValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushBackResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListByteArray()).hasSize(1).containsOnly(oldValue));

    // Add the same value
    assertThat(cacheClient.listPushBack(cacheName, listName, oldValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushBackResponse.Success.class);

    final List<byte[]> expectedList = Arrays.asList(oldValue, oldValue);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(2)
                    .containsExactlyElementsOf(expectedList));

    // Add a new value and truncate the list to 2 items
    assertThat(cacheClient.listPushBack(cacheName, listName, newValue, 2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushBackResponse.Success.class);

    final List<byte[]> newExpectedList = Arrays.asList(oldValue, newValue);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(2)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushBackWhenNullCacheName() {
    final String listName = randomString();
    final String stringValue = "val1";
    final byte[] byteArrayValue = "val1".getBytes();

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushBack(null, listName, stringValue, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushBack(null, listName, stringValue, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushBack(
                null, listName, byteArrayValue, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushBack(null, listName, byteArrayValue, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushBackWhenNullListName() {
    final String stringValue = "val1";
    final byte[] byteArrayValue = "val1".getBytes();

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushBack(cacheName, null, stringValue, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushBack(cacheName, null, stringValue, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushBack(
                cacheName, null, byteArrayValue, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushBack(cacheName, null, byteArrayValue, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushBackWhenNullElement() {
    final String listName = randomString();
    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushBack(
                cacheName, listName, (String) null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushBack(cacheName, listName, (String) null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushBack(
                cacheName, listName, (byte[]) null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushBack(cacheName, listName, (byte[]) null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushBackResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listPushFrontStringHappyPath() {
    final String listName = randomString();
    final String oldValue = "val1";
    final String newValue = "val2";

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listPushFront(
                cacheName, listName, oldValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(1).containsOnly(oldValue));

    // Add the same value
    assertThat(cacheClient.listPushFront(cacheName, listName, oldValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushFrontResponse.Success.class);

    final List<String> expectedList = Arrays.asList(oldValue, oldValue);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(2)
                    .containsExactlyElementsOf(expectedList));

    // Add a new value and truncate the list to 2 items
    assertThat(cacheClient.listPushFront(cacheName, listName, newValue, 2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushFrontResponse.Success.class);

    final List<String> newExpectedList = Arrays.asList(newValue, oldValue);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListString())
                    .hasSize(2)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @Test
  public void listPushFrontByteArrayHappyPath() {
    final String listName = randomString();
    final byte[] oldValue = "val1".getBytes();
    final byte[] newValue = "val2".getBytes();

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListFetchResponse.Miss.class);

    assertThat(
            cacheClient.listPushFront(
                cacheName, listName, oldValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListByteArray()).hasSize(1).containsOnly(oldValue));

    // Add the same value
    assertThat(cacheClient.listPushFront(cacheName, listName, oldValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushFrontResponse.Success.class);

    final List<byte[]> expectedList = Arrays.asList(oldValue, oldValue);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(2)
                    .containsExactlyElementsOf(expectedList));

    // Add a new value and truncate the list to 2 items
    assertThat(cacheClient.listPushFront(cacheName, listName, newValue, 2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListPushFrontResponse.Success.class);

    final List<byte[]> newExpectedList = Arrays.asList(newValue, oldValue);
    assertThat(cacheClient.listFetch(cacheName, listName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueListByteArray())
                    .hasSize(2)
                    .containsExactlyElementsOf(newExpectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushFrontWhenNullCacheName() {
    final String listName = randomString();
    final String stringValue = "val1";
    final byte[] byteArrayValue = "val1".getBytes();

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushFront(
                null, listName, stringValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushFront(null, listName, stringValue, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushFront(
                null, listName, byteArrayValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushFront(null, listName, byteArrayValue, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushFrontWhenNullListName() {
    final String stringValue = "val1";
    final byte[] byteArrayValue = "val1".getBytes();

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushFront(
                cacheName, null, stringValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushFront(cacheName, null, stringValue, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushFront(
                cacheName, null, byteArrayValue, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushFront(cacheName, null, byteArrayValue, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListPushFrontWhenNullElement() {
    final String listName = randomString();
    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushFront(
                cacheName, listName, (String) null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushFront(cacheName, listName, (String) null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // With ttl specified in method signature
    assertThat(
            cacheClient.listPushFront(
                cacheName, listName, (byte[]) null, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Without ttl specified in method signature
    assertThat(cacheClient.listPushFront(cacheName, listName, (byte[]) null, 0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListPushFrontResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void listRemoveValueStringHappyPath() {
    final String listName = randomString();
    List<String> values = Arrays.asList("val1", "val1", "val2", "val3", "val4");

    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, values, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    // Remove value from list
    String removeValue = "val1";
    assertThat(cacheClient.listRemoveValue(cacheName, listName, removeValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListRemoveValueResponse.Success.class);

    List<String> expectedList = Arrays.asList("val2", "val3", "val4");
    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(3).containsAll(expectedList));
  }

  @Test
  public void listRemoveValueByteArrayHappyPath() {
    final String listName = randomString();
    List<byte[]> values =
        Arrays.asList(
            "val1".getBytes(),
            "val1".getBytes(),
            "val2".getBytes(),
            "val3".getBytes(),
            "val4".getBytes());

    assertThat(
            cacheClient.listConcatenateFrontByteArray(
                cacheName, listName, values, 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    // Remove value from list
    byte[] removeValue = "val1".getBytes();
    assertThat(cacheClient.listRemoveValue(cacheName, listName, removeValue))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListRemoveValueResponse.Success.class);

    List<byte[]> expectedList =
        Arrays.asList("val2".getBytes(), "val3".getBytes(), "val4".getBytes());
    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListByteArray()).hasSize(3).containsAll(expectedList));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListRemoveValueWhenNullCacheName() {
    final String listName = randomString();
    String stringValue = "val1";
    byte[] byteArrayValue = "val1".getBytes();

    assertThat(cacheClient.listRemoveValue(null, listName, stringValue))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.listRemoveValue(null, listName, byteArrayValue))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListRemoveValueWhenNullListName() {
    String stringValue = "val1";
    byte[] byteArrayValue = "val1".getBytes();

    assertThat(cacheClient.listRemoveValue(cacheName, null, stringValue))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.listRemoveValue(cacheName, null, byteArrayValue))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldFailListRemoveValueWhenNullElement() {
    final String listName = randomString();
    assertThat(cacheClient.listRemoveValue(cacheName, listName, (String) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.listRemoveValue(null, listName, (byte[]) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListRemoveValueResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithPositiveStartEndIndices() {
    final String listName = randomString();
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3", "val4");

    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(4).containsAll(stringValues));

    assertThat(cacheClient.listRetain(cacheName, listName, 1, 3))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListRetainResponse.Success.class);

    List<String> expectedList = Arrays.asList("val2", "val3");
    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(2).containsAll(expectedList));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithNegativeStartEndIndices() {
    final String listName = randomString();
    final List<String> stringValues = Arrays.asList("val1", "val2", "val3", "val4");

    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(4).containsAll(stringValues));

    assertThat(cacheClient.listRetain(cacheName, listName, -3, -1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListRetainResponse.Success.class);

    List<String> expectedList = Arrays.asList("val2", "val3");
    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(2).containsAll(expectedList));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithNullStartIndex() {
    final String listName = randomString();
    final List<String> stringValues =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8");

    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));

    // valid case for null startIndex and positive endIndex
    assertThat(cacheClient.listRetain(cacheName, listName, null, 7))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListRetainResponse.Success.class);

    List<String> expectedList =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7");
    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(7).containsAll(expectedList));

    // valid case for null startIndex and negative endIndex
    assertThat(cacheClient.listRetain(cacheName, listName, null, -3))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListRetainResponse.Success.class);

    List<String> newExpectedList = Arrays.asList("val1", "val2", "val3", "val4");
    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListString()).hasSize(4).containsAll(newExpectedList));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithNullEndIndex() {
    final String listName = randomString();
    final List<String> stringValues =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8");

    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));

    // valid case for positive startIndex and null endIndex
    assertThat(cacheClient.listRetain(cacheName, listName, 2, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListRetainResponse.Success.class);

    List<String> expectedList = Arrays.asList("val3", "val4", "val5", "val6", "val7", "val8");
    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(6).containsAll(expectedList));

    // valid case for negative startIndex and null endIndex
    assertThat(cacheClient.listRetain(cacheName, listName, -4, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListRetainResponse.Success.class);

    List<String> newExpectedList = Arrays.asList("val5", "val6", "val7", "val8");
    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueListString()).hasSize(4).containsAll(newExpectedList));
  }

  @Test
  public void shouldRetainAllValuesWhenListRetainWithNullStartAndEndIndices() {
    final String listName = randomString();
    final List<String> stringValues =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8");

    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));

    // valid case for null startIndex and null endIndex
    assertThat(cacheClient.listRetain(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListRetainResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void shouldRetainAllValuesWhenListRetainWithInvalidIndices() {
    final String listName = randomString();
    final List<String> stringValues =
        Arrays.asList("val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8");

    assertThat(
            cacheClient.listConcatenateFront(
                cacheName, listName, stringValues, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(ListConcatenateFrontResponse.Success.class);

    assertThat(cacheClient.listFetch(cacheName, listName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueListString()).hasSize(8).containsAll(stringValues));

    // the positive startIndex is larger than the positive endIndex
    assertThat(cacheClient.listRetain(null, listName, 3, 1))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListRetainResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // the positive startIndex is the same value as the positive endIndex
    assertThat(cacheClient.listRetain(null, listName, 3, 3))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListRetainResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // the negative startIndex is the larger than the negative endIndex
    assertThat(cacheClient.listRetain(null, listName, -3, -5))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListRetainResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }
}
