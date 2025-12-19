package momento.sdk.cache;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import momento.sdk.exceptions.CacheAlreadyExistsException;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.requests.CollectionTtl;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetBatchResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.IncrementResponse;
import momento.sdk.responses.cache.SetBatchResponse;
import momento.sdk.responses.cache.SetIfNotExistsResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import momento.sdk.responses.cache.control.CacheFlushResponse;
import momento.sdk.responses.cache.control.CacheListResponse;
import momento.sdk.responses.cache.dictionary.DictionaryFetchResponse;
import momento.sdk.responses.cache.dictionary.DictionaryGetFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionaryIncrementResponse;
import momento.sdk.responses.cache.dictionary.DictionaryRemoveFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionarySetFieldResponse;
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
import momento.sdk.responses.cache.set.SetAddElementsResponse;
import momento.sdk.responses.cache.set.SetFetchResponse;
import momento.sdk.responses.cache.set.SetRemoveElementResponse;
import momento.sdk.responses.cache.sortedset.ScoredElement;
import momento.sdk.responses.cache.sortedset.SortedSetFetchResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetRankResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetScoreResponse;
import momento.sdk.responses.cache.sortedset.SortedSetIncrementScoreResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementsResponse;
import momento.sdk.responses.cache.sortedset.SortedSetRemoveElementsResponse;
import momento.sdk.responses.cache.ttl.ItemGetTtlResponse;
import momento.sdk.responses.cache.ttl.UpdateTtlResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

final class CacheTestApiKeyV2 extends BaseCacheTestClass {

  // control plane
  @Test
  public void listsCachesHappyPath() {
    final String newCache = randomString("name");

    assertThat(cacheClient.createCache(newCache))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheCreateResponse.Success.class);

    try {
      assertThat(cacheClient.listCaches())
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CacheListResponse.Success.class))
          .satisfies(
              success ->
                  assertThat(success.getCaches()).anyMatch(ci -> ci.name().equals(newCache)));
    } finally {
      // cleanup
      assertThat(cacheClient.deleteCache(newCache))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(CacheDeleteResponse.Success.class);
    }
  }

  @Test
  public void createDeleteCache_HappyPath() {
    final String newCache = randomString("java-v2");

    assertThat(cacheClient.createCache(newCache))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheCreateResponse.Success.class);

    assertThat(cacheClient.createCache(newCache))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheCreateResponse.Error.class))
        .satisfies(
            error -> assertThat(error).hasCauseInstanceOf(CacheAlreadyExistsException.class));

    assertThat(cacheClient.deleteCache(newCache))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDeleteResponse.Success.class);

    assertThat(cacheClient.deleteCache(newCache))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDeleteResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void shouldFlushCacheContents() {
    final String cacheToFlush = randomString("cacheToFlush");
    final String key = randomString();
    final String value = randomString();
    final Duration ttl1Hour = Duration.ofHours(1);

    try {
      CacheCreateResponse response = cacheClient.createCache(cacheToFlush).join();
      assertThat(response).isInstanceOf(CacheCreateResponse.Success.class);
      assertThat(cacheClient.set(cacheName, key, value, ttl1Hour))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(SetResponse.Success.class))
          .satisfies(success -> assertThat(success.value()).isEqualTo(value));

      // Execute Flush
      assertThat(cacheClient.flushCache(cacheName))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(CacheFlushResponse.Success.class);

      // Verify that previously set key is now a MISS
      assertThat(cacheClient.get(cacheName, key))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(GetResponse.Miss.class);
    } finally {
      cacheClient.deleteCache(cacheToFlush).join();
    }
  }

  // get set delete
  @Test
  public void createCacheGetSetDeleteValuesAndDeleteCache() {
    final String alternateCacheName = randomString("alternateName");
    final String key = randomString("key");
    final String value = randomString("value");

    cacheClient.createCache(alternateCacheName).join();
    try {
      cacheClient.set(cacheName, key, value).join();

      final GetResponse getResponse = cacheClient.get(cacheName, key).join();
      assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
      assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);

      final DeleteResponse deleteResponse = cacheClient.delete(cacheName, key).join();
      assertThat(deleteResponse).isInstanceOf(DeleteResponse.Success.class);

      final GetResponse getAfterDeleteResponse = cacheClient.get(cacheName, key).join();
      assertThat(getAfterDeleteResponse).isInstanceOf(GetResponse.Miss.class);

      final GetResponse getForKeyInSomeOtherCache = cacheClient.get(alternateCacheName, key).join();
      assertThat(getForKeyInSomeOtherCache).isInstanceOf(GetResponse.Miss.class);
    } finally {
      cacheClient.deleteCache(alternateCacheName).join();
    }
  }

  @Test
  public void shouldSetStringValueToStringKeyWhenKeyNotExistsWithTtl() {
    final String key = randomString();
    final String value = randomString();

    SetIfNotExistsResponse setIfNotExistsResponse =
        cacheClient.setIfNotExists(cacheName, key, value, DEFAULT_TTL_SECONDS).join();

    assertThat(setIfNotExistsResponse).isInstanceOf(SetIfNotExistsResponse.Stored.class);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).keyString()).isEqualTo(key);
    assertThat(((SetIfNotExistsResponse.Stored) setIfNotExistsResponse).valueString())
        .isEqualTo(value);

    GetResponse getResponse = cacheClient.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }

  @Test
  public void getBatchSetBatchHappyPath() {
    final Map<String, String> items = new HashMap<>();
    items.put("key1", "val1");
    items.put("key2", "val2");
    items.put("key3", "val3");
    final SetBatchResponse setBatchResponse =
        cacheClient.setBatch(cacheName, items, Duration.ofMinutes(1)).join();
    assertThat(setBatchResponse).isInstanceOf(SetBatchResponse.Success.class);
    for (SetResponse setResponse :
        ((SetBatchResponse.Success) setBatchResponse).results().values()) {
      assertThat(setResponse).isInstanceOf(SetResponse.Success.class);
    }

    final GetBatchResponse getBatchResponse =
        cacheClient.getBatch(cacheName, items.keySet()).join();

    assertThat(getBatchResponse).isInstanceOf(GetBatchResponse.Success.class);
    assertThat(((GetBatchResponse.Success) getBatchResponse).valueMapStringString())
        .containsExactlyEntriesOf(items);
  }

  // other cache methods
  @Test
  public void shouldUpdateTTLAndGetItWithStringKey() {
    final String key = randomString();

    // set a key with default ttl
    SetResponse setResponse = cacheClient.set(cacheName, key, "value", DEFAULT_TTL_SECONDS).join();

    assertThat(setResponse).isInstanceOf(SetResponse.Success.class);

    ItemGetTtlResponse itemGetTtlResponse = cacheClient.itemGetTtl(cacheName, key).join();

    // retrieved ttl should work and less than default ttl
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).remainingTtlMillis())
        .isLessThan(DEFAULT_TTL_SECONDS.toMillis());

    // update ttl to 300 seconds
    Duration updatedTTL = Duration.of(300, ChronoUnit.SECONDS);
    UpdateTtlResponse updateTtlResponse = cacheClient.updateTtl(cacheName, key, updatedTTL).join();

    assertThat(updateTtlResponse).isInstanceOf(UpdateTtlResponse.Set.class);

    itemGetTtlResponse = cacheClient.itemGetTtl(cacheName, key).join();

    // assert that the updated ttl is less than 300 seconds but more than 300 - epsilon (taken as 60
    // to reduce flakiness)
    assertThat(itemGetTtlResponse).isInstanceOf(ItemGetTtlResponse.Hit.class);
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).remainingTtlMillis())
        .isLessThan(updatedTTL.toMillis());
    assertThat(((ItemGetTtlResponse.Hit) itemGetTtlResponse).remainingTtlMillis())
        .isGreaterThan(updatedTTL.minusSeconds(60).toMillis());
  }

  @Test
  public void shouldReturnCacheIncrementedValuesWithStringField() {
    final String field = randomString();

    IncrementResponse incrementResponse =
        cacheClient.increment(cacheName, field, 1, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(1);

    // increment with ttl specified
    incrementResponse = cacheClient.increment(cacheName, field, 50, DEFAULT_TTL_SECONDS).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(51);

    // increment without ttl specified
    incrementResponse = cacheClient.increment(cacheName, field, -1051).join();

    assertThat(incrementResponse).isInstanceOf(IncrementResponse.Success.class);
    assertThat(((IncrementResponse.Success) incrementResponse).valueNumber()).isEqualTo(-1000);

    GetResponse getResp = cacheClient.get(cacheName, field).join();
    assertThat(getResp).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResp).valueString()).isEqualTo("-1000");
  }

  // dictionary
  @Test
  public void dictionarySetFieldAndDictionaryFetchAndHappyPath() {
    final String dictionaryName = randomString();

    // Set String key, String Value
    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(cacheClient.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueMapStringString()).hasSize(1).containsEntry("a", "b"));

    // Set String key, ByteArray Value
    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "c", "d".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(cacheClient.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueMapStringByteArray())
                    .hasSize(2)
                    .containsEntry("c", "d".getBytes()));
  }

  @Test
  public void dictionaryGetFieldHappyPath() {
    final String dictionaryName = randomString();

    // Get the value as a string
    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(cacheClient.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.fieldString()).isEqualTo("a");
              assertThat(hit.valueString()).isEqualTo("b");
            });

    // Get the value as a byte array
    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "c", "d".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(cacheClient.dictionaryGetField(cacheName, dictionaryName, "c"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.field()).isEqualTo("c");
              assertThat(hit.valueByteArray()).isEqualTo("d".getBytes());
            });
  }

  @Test
  public void dictionaryIncrementStringFieldHappyPath() {
    final String dictionaryName = randomString();

    // Increment with ttl
    assertThat(cacheClient.dictionaryIncrement(cacheName, dictionaryName, "a", 1))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.value()).isEqualTo(1));

    assertThat(
            cacheClient.dictionaryIncrement(
                cacheName, dictionaryName, "a", 41, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.value()).isEqualTo(42));

    // Increment without ttl
    assertThat(
            cacheClient.dictionaryIncrement(
                cacheName, dictionaryName, "a", -1042, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.value()).isEqualTo(-1000));

    assertThat(cacheClient.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.fieldString()).isEqualTo("a");
              assertThat(hit.valueString()).isEqualTo("-1000");
            });
  }

  @Test
  public void dictionaryRemoveFieldStringHappyPath() {
    final String dictionaryName = randomString();

    assertThat(cacheClient.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryGetFieldResponse.Miss.class);

    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(cacheClient.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.fieldString()).isEqualTo("a");
              assertThat(hit.valueString()).isEqualTo("b");
            });

    assertThat(cacheClient.dictionaryRemoveField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryRemoveFieldResponse.Success.class);

    assertThat(cacheClient.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryGetFieldResponse.Miss.class);
  }

  // list
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
  public void shouldFetchAllValuesWhenListFetchWithPositiveStartEndIndices() {
    List<String> values = Arrays.asList("val1", "val2", "val3", "val4");
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

  // set
  @Test
  public void setAddElementsStringHappyPath() {
    final String setName = randomString();
    final Set<String> firstSet = Sets.newHashSet("one", "two");
    final Set<String> secondSet = Sets.newHashSet("two", "three");

    assertThat(cacheClient.setAddElements(cacheName, setName, firstSet))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(2).containsAll(firstSet));

    // Try to add the same elements again
    assertThat(
            cacheClient.setAddElements(cacheName, setName, firstSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(2).containsAll(firstSet));

    // Add a set with one new and one overlapping element
    assertThat(
            cacheClient.setAddElements(cacheName, setName, secondSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueSetString())
                    .hasSize(3)
                    .containsAll(firstSet)
                    .containsAll(secondSet));
  }

  @Test
  public void setRemoveElementStringHappyPath() {
    final String setName = randomString();
    final String element1 = "one";
    final String element2 = "two";
    final Set<String> elements = Sets.newHashSet(element1, element2);

    // Add some elements to a set
    assertThat(
            cacheClient.setAddElements(cacheName, setName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueSetString()).hasSize(2).containsOnly(element1, element2));

    // Remove an element
    assertThat(cacheClient.setRemoveElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element2));

    // Try to remove the same element again
    assertThat(cacheClient.setRemoveElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element2));

    // Remove the last element
    assertThat(cacheClient.setRemoveElement(cacheName, setName, element2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetFetchResponse.Miss.class);

    // Remove an element from the now non-existent set
    assertThat(cacheClient.setRemoveElement(cacheName, setName, element2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetFetchResponse.Miss.class);
  }

  // sorted set
  @Test
  public void sortedSetPutElementStringHappyPath() {
    final String sortedSetName = randomString();
    final String value = "1";
    final double score = 1.0;

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(
            cacheClient.sortedSetPutElement(
                cacheName, sortedSetName, value, score, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).map(ScoredElement::getValue).containsOnly(value);
              assertThat(scoredElements).map(ScoredElement::getScore).containsOnly(score);
            });
  }

  @Test
  public void sortedSetFetchByRankStringHappyPath() {
    final String sortedSetName = randomString();
    final String one = "1";
    final String two = "2";
    final String three = "3";
    final String four = "4";
    final String five = "5";

    final Map<String, Double> elements = new HashMap<>();
    elements.put(one, 0.0);
    elements.put(two, 1.0);
    elements.put(three, 0.5);
    elements.put(four, 2.0);
    elements.put(five, 1.5);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(cacheClient.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    // Full set ascending, end index larger than set
    assertThat(
            cacheClient.sortedSetFetchByRank(cacheName, sortedSetName, 0, 6, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(5);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getValue)
                  .containsSequence(one, three, two, five, four);
            });

    // Partial set descending
    assertThat(
            cacheClient.sortedSetFetchByRank(cacheName, sortedSetName, 1, 4, SortOrder.DESCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(3);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getValue)
                  .containsSequence(five, two, three);
            });
  }

  @Test
  public void sortedSetFetchByScoreStringHappyPath() {
    final String sortedSetName = randomString();
    final String one = "1";
    final String two = "2";
    final String three = "3";
    final String four = "4";
    final String five = "5";

    final Map<String, Double> elements = new HashMap<>();
    elements.put(one, 0.0);
    elements.put(two, 1.0);
    elements.put(three, 0.5);
    elements.put(four, 2.0);
    elements.put(five, 1.5);

    assertThat(cacheClient.sortedSetFetchByScore(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(cacheClient.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    // Full set ascending, end index larger than set
    assertThat(
            cacheClient.sortedSetFetchByScore(
                cacheName, sortedSetName, 0.0, 9.9, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(5);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getValue)
                  .containsSequence(one, three, two, five, four);
              assertThat(scoredElements)
                  .map(ScoredElement::getScore)
                  .containsSequence(0.0, 0.5, 1.0, 1.5, 2.0);
            });

    // Partial set descending
    assertThat(
            cacheClient.sortedSetFetchByScore(
                cacheName, sortedSetName, 0.2, 1.9, SortOrder.DESCENDING, 0, 99))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(3);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getValue)
                  .containsSequence(five, two, three);
            });

    // Partial set limited by offset and count
    assertThat(cacheClient.sortedSetFetchByScore(cacheName, sortedSetName, null, null, null, 1, 3))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(3);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getValue)
                  .containsSequence(three, two, five);
            });

    // Full set ascending
    assertThat(cacheClient.sortedSetFetchByScore(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(5);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getValue)
                  .containsSequence(one, three, two, five, four);
            });
  }

  @Test
  public void sortedSetGetRankStringHappyPath() {
    final String sortedSetName = randomString();
    final String one = "1";
    final String two = "2";

    assertThat(cacheClient.sortedSetGetRank(cacheName, sortedSetName, one, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetGetRankResponse.Miss.class);

    assertThat(cacheClient.sortedSetPutElement(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(cacheClient.sortedSetGetRank(cacheName, sortedSetName, one, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.rank()).isEqualTo(0));

    // Add another element that changes the rank of the first one
    assertThat(cacheClient.sortedSetPutElement(cacheName, sortedSetName, two, 0.5))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(cacheClient.sortedSetGetRank(cacheName, sortedSetName, one, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.rank()).isEqualTo(1));

    // Check the descending rank
    assertThat(cacheClient.sortedSetGetRank(cacheName, sortedSetName, one, SortOrder.DESCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.rank()).isEqualTo(0));
  }

  @Test
  public void sortedSetGetScoreStringHappyPath() {
    final String sortedSetName = randomString();
    final String one = "1";
    final String two = "2";

    assertThat(cacheClient.sortedSetGetScore(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetGetScoreResponse.Miss.class);

    assertThat(cacheClient.sortedSetPutElement(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(cacheClient.sortedSetGetScore(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.score()).isEqualTo(1.0));

    // Add another element that changes the rank of the first one
    assertThat(cacheClient.sortedSetPutElement(cacheName, sortedSetName, two, 0.5))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(cacheClient.sortedSetGetScore(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.score()).isEqualTo(1.0));
  }

  @Test
  public void sortedSetIncrementScoreStringHappyPath() {
    final String sortedSetName = randomString();
    final String one = "1";

    assertThat(cacheClient.sortedSetIncrementScore(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Success.class))
        .satisfies(success -> assertThat(success.score()).isEqualTo(1.0));

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.elementsList())
                    .hasSize(1)
                    .map(ScoredElement::getScore)
                    .containsOnly(1.0));

    assertThat(cacheClient.sortedSetIncrementScore(cacheName, sortedSetName, one, 14.5))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Success.class))
        .satisfies(success -> assertThat(success.score()).isEqualTo(15.5));

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.elementsList())
                    .hasSize(1)
                    .map(ScoredElement::getScore)
                    .containsOnly(15.5));

    assertThat(cacheClient.sortedSetIncrementScore(cacheName, sortedSetName, one, -115.5))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Success.class))
        .satisfies(success -> assertThat(success.score()).isEqualTo(-100));
  }

  @Test
  public void sortedSetRemoveElementsStringHappyPath() {
    final String sortedSetName = randomString();
    final String one = "1";
    final String two = "2";
    final String three = "3";
    final Map<String, Double> elements = ImmutableMap.of(one, 1.0, two, 2.0, three, 3.0);

    assertThat(cacheClient.sortedSetRemoveElements(cacheName, sortedSetName, elements.keySet()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(
            cacheClient.sortedSetRemoveElements(
                cacheName, sortedSetName, Sets.newHashSet(one, two)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.elementsList())
                    .hasSize(1)
                    .map(ScoredElement::getValue)
                    .containsOnly(three));
  }
}
