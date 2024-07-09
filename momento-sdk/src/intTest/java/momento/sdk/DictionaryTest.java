package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import momento.sdk.exceptions.BadRequestException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.requests.CollectionTtl;
import momento.sdk.responses.cache.dictionary.DictionaryFetchResponse;
import momento.sdk.responses.cache.dictionary.DictionaryGetFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionaryGetFieldsResponse;
import momento.sdk.responses.cache.dictionary.DictionaryIncrementResponse;
import momento.sdk.responses.cache.dictionary.DictionaryRemoveFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionaryRemoveFieldsResponse;
import momento.sdk.responses.cache.dictionary.DictionarySetFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionarySetFieldsResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class DictionaryTest extends BaseTestClass {
  Map<String, String> stringStringMap = new HashMap<>();
  Map<String, byte[]> stringBytesMap = new HashMap<>();
  Map<byte[], String> bytesStringMap = new HashMap<>();
  Map<byte[], byte[]> bytesBytesMap = new HashMap<>();

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
  public void dictionarySetFieldAndDictionaryFetchAndHappyPathWithNoTtl() {
    final String dictionaryName = randomString();

    // Set String key, String Value
    assertThat(cacheClient.dictionarySetField(cacheName, dictionaryName, "a", "b"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(cacheClient.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueMapStringString()).hasSize(1).containsEntry("a", "b"));

    // Set String key, ByteArray Value
    assertThat(cacheClient.dictionarySetField(cacheName, dictionaryName, "c", "d".getBytes()))
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

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionarySetFieldReturnsErrorWithNullCacheName() {
    final String dictionaryName = randomString();

    // String Key and String value
    assertThat(
            cacheClient.dictionarySetField(
                null, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            cacheClient.dictionarySetField(
                null, dictionaryName, "a", "b".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionarySetFieldReturnsErrorWithNullDictionaryName() {
    // String Key and String value
    assertThat(
            cacheClient.dictionarySetField(cacheName, null, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            cacheClient.dictionarySetField(
                cacheName, null, "a", "b".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionarySetFieldReturnsErrorWithNullField() {
    final String dictionaryName = randomString();

    // String Key and String value
    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, null, "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, null, "b".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionarySetFieldReturnsErrorWithNullValue() {
    final String dictionaryName = randomString();

    // String Key and String value
    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "a", (String) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "a", (byte[]) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryFetchReturnsErrorWithNullCacheName() {
    final String dictionaryName = randomString();

    assertThat(cacheClient.dictionaryFetch(null, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryFetchReturnsErrorWithNullDictionaryName() {
    assertThat(cacheClient.dictionaryFetch(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryFetchReturnsMissWhenDictionaryNotExists() {
    final String dictionaryName = randomString();

    assertThat(cacheClient.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryFetchResponse.Miss.class);
  }

  @Test
  public void dictionarySetFieldsAndDictionaryFetchAndHappyPath() {
    final String dictionaryName = randomString();

    populateTestMaps();

    // Set String key, String value
    assertThat(
            cacheClient.dictionarySetFields(
                cacheName, dictionaryName, stringStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldsResponse.Success.class);

    assertThat(cacheClient.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, String> stringStringMap = hit.valueMapStringString();
              assertThat(stringStringMap.keySet()).hasSize(2).contains("a", "aa");
              assertThat(stringStringMap.values()).contains("bb", "bb");
            });

    // Set String key, byte array value
    assertThat(
            cacheClient.dictionarySetFieldsStringBytes(
                cacheName, dictionaryName, stringBytesMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldsResponse.Success.class);

    assertThat(cacheClient.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, byte[]> stringBytesMap = hit.valueMapStringByteArray();
              assertThat(stringBytesMap.keySet()).hasSize(4).contains("c", "cc");
              assertThat(stringBytesMap.values()).contains("d".getBytes(), "dd".getBytes());
            });
  }

  @Test
  public void dictionarySetFieldsAndDictionaryFetchAndHappyPathWithNoTtl() {
    final String dictionaryName = randomString();

    populateTestMaps();

    // Set String key, String value
    assertThat(cacheClient.dictionarySetFields(cacheName, dictionaryName, stringStringMap))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldsResponse.Success.class);

    assertThat(cacheClient.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, String> stringStringMap = hit.valueMapStringString();
              assertThat(stringStringMap.keySet()).hasSize(2).contains("a", "aa");
              assertThat(stringStringMap.values()).contains("b", "bb");
            });

    // Set String key, byte array value
    assertThat(
            cacheClient.dictionarySetFieldsStringBytes(cacheName, dictionaryName, stringBytesMap))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldsResponse.Success.class);

    assertThat(cacheClient.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, byte[]> stringBytesMap = hit.valueMapStringByteArray();
              assertThat(stringBytesMap.keySet()).hasSize(4).contains("c", "cc");
              assertThat(stringBytesMap.values()).contains("d".getBytes(), "dd".getBytes());
            });
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionarySetFieldsReturnsErrorWithNullCacheName() {
    final String dictionaryName = randomString();

    populateTestMaps();

    // String Key and String value
    assertThat(
            cacheClient.dictionarySetFields(
                null, dictionaryName, stringStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            cacheClient.dictionarySetFieldsStringBytes(
                null, dictionaryName, stringBytesMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionarySetFieldsReturnsErrorWithNullDictionaryName() {
    populateTestMaps();

    // String Key and String value
    assertThat(
            cacheClient.dictionarySetFields(
                cacheName, null, stringStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            cacheClient.dictionarySetFieldsStringBytes(
                cacheName, null, stringBytesMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionarySetFieldsReturnsErrorWithNullItem() {
    final String dictionaryName = randomString();

    // String Key and String value
    assertThat(
            cacheClient.dictionarySetFields(
                cacheName, dictionaryName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            cacheClient.dictionarySetFieldsStringBytes(
                cacheName, dictionaryName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
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

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryGetFieldReturnsErrorWithNullCacheName() {
    final String dictionaryName = randomString();

    // String field
    assertThat(cacheClient.dictionaryGetField(null, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryGetFieldReturnsErrorWithNullDictionaryName() {
    // String field
    assertThat(cacheClient.dictionaryGetField(cacheName, null, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryGetFieldReturnsErrorWithNullField() {
    final String dictionaryName = randomString();

    // String field
    assertThat(cacheClient.dictionaryGetField(cacheName, dictionaryName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldReturnsMissWhenFieldNotExists() {
    final String dictionaryName = randomString();

    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    // String field
    assertThat(cacheClient.dictionaryGetField(cacheName, dictionaryName, "c"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryGetFieldResponse.Miss.class);
  }

  @Test
  public void dictionaryGetFieldReturnsMissWhenDictionaryNotExists() {
    final String dictionaryName = randomString();

    // String field
    assertThat(cacheClient.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryGetFieldResponse.Miss.class);
  }

  @Test
  public void dictionaryGetFieldsStringHappyPath() {
    final String dictionaryName = randomString();

    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "c", "d", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "e", "f", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    // Gets a map of string keys and string values
    assertThat(
            cacheClient.dictionaryGetFields(
                cacheName, dictionaryName, Arrays.asList("a", "c", "e")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldsResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, String> stringStringMap = hit.valueMapStringString();
              assertThat(stringStringMap.keySet()).hasSize(3).contains("a", "c", "e");
              assertThat(stringStringMap.values()).contains("b", "d", "f");
            });

    // Gets a map of string keys and byte array values
    assertThat(
            cacheClient.dictionaryGetFields(
                cacheName, dictionaryName, Arrays.asList("a", "c", "e")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldsResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, byte[]> stringStringMap = hit.valueMapStringByteArray();
              assertThat(stringStringMap.keySet()).hasSize(3).contains("a", "c", "e");
              assertThat(stringStringMap.values())
                  .contains("b".getBytes(), "d".getBytes(), "f".getBytes());
            });
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryGetFieldsReturnsErrorWithNullCacheName() {
    final String dictionaryName = randomString();

    // String fields
    assertThat(cacheClient.dictionaryGetFields(null, dictionaryName, Arrays.asList("a", "c", "e")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryGetFieldsReturnsErrorWithNullDictionaryName() {
    // String fields
    assertThat(cacheClient.dictionaryGetFields(cacheName, null, Arrays.asList("a", "c", "e")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryGetFieldsReturnsErrorWithNullFields() {
    final String dictionaryName = randomString();

    // String fields
    assertThat(cacheClient.dictionaryGetFields(cacheName, dictionaryName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    List<String> stringListWithAtleastOneNullField = Arrays.asList("a", null);

    assertThat(
            cacheClient.dictionaryGetFields(
                cacheName, dictionaryName, stringListWithAtleastOneNullField))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldsReturnsMissOrHitWhenFieldsNotExistsOrExistsRespectively() {
    final String dictionaryName = randomString();

    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(
            cacheClient.dictionarySetField(
                cacheName, dictionaryName, "c", "d", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    // get raw responses and validate Hit/Miss
    DictionaryGetFieldsResponse response =
        cacheClient
            .dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "c", "r"))
            .join();

    assertThat(response).isInstanceOf(DictionaryGetFieldsResponse.Hit.class);

    List<DictionaryGetFieldResponse> responseList =
        ((DictionaryGetFieldsResponse.Hit) response).perFieldResponses();
    assertThat(responseList).hasSize(3);

    assertThat(responseList.get(0)).isInstanceOf(DictionaryGetFieldResponse.Hit.class);
    assertThat(((DictionaryGetFieldResponse.Hit) responseList.get(0)).fieldString()).isEqualTo("a");
    assertThat(((DictionaryGetFieldResponse.Hit) responseList.get(0)).valueString()).isEqualTo("b");
    assertThat(((DictionaryGetFieldResponse.Hit) responseList.get(0)).valueByteArray())
        .isEqualTo("b".getBytes());

    assertThat(((DictionaryGetFieldsResponse.Hit) response).perFieldResponses().get(1))
        .isInstanceOf(DictionaryGetFieldResponse.Hit.class);
    assertThat(((DictionaryGetFieldResponse.Hit) responseList.get(1)).fieldString()).isEqualTo("c");
    assertThat(((DictionaryGetFieldResponse.Hit) responseList.get(1)).valueString()).isEqualTo("d");
    assertThat(((DictionaryGetFieldResponse.Hit) responseList.get(1)).valueByteArray())
        .isEqualTo("d".getBytes());

    assertThat(((DictionaryGetFieldsResponse.Hit) response).perFieldResponses().get(2))
        .isInstanceOf(DictionaryGetFieldResponse.Miss.class);
    assertThat(((DictionaryGetFieldResponse.Miss) responseList.get(2)).fieldString())
        .isEqualTo("r");
    assertThat(((DictionaryGetFieldResponse.Miss) responseList.get(2)).fieldByteArray())
        .isEqualTo("r".getBytes());
  }

  @Test
  public void dictionaryGetFieldsReturnsMissWhenDictionaryNotExists() {
    final String dictionaryName = randomString();

    // String field
    assertThat(
            cacheClient.dictionaryGetFields(
                cacheName, dictionaryName, Collections.singletonList("a")))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryGetFieldsResponse.Miss.class);
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
  public void dictionaryIncrementSetAndResetHappyPath() {
    final String dictionaryName = randomString();

    // Set field
    assertThat(cacheClient.dictionarySetField(cacheName, dictionaryName, "a", "10"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(
            cacheClient.dictionaryIncrement(
                cacheName, dictionaryName, "a", 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.value()).isEqualTo(10));

    assertThat(
            cacheClient.dictionaryIncrement(
                cacheName, dictionaryName, "a", 90, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.value()).isEqualTo(100));

    // Reset field
    assertThat(cacheClient.dictionarySetField(cacheName, dictionaryName, "a", "0"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(
            cacheClient.dictionaryIncrement(
                cacheName, dictionaryName, "a", 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.value()).isEqualTo(0));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryIncrementReturnsErrorWithNullCacheName() {
    final String dictionaryName = randomString();

    // String field
    assertThat(
            cacheClient.dictionaryIncrement(
                null, dictionaryName, "a", 1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryIncrementReturnsErrorWithNullDictionaryName() {
    // String field
    assertThat(
            cacheClient.dictionaryIncrement(cacheName, null, "a", 1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryIncrementReturnsErrorWithNullField() {
    final String dictionaryName = randomString();

    // String field
    assertThat(
            cacheClient.dictionaryIncrement(
                cacheName, dictionaryName, null, 1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryIncrementReturnsBadRequestError() {
    final String dictionaryName = randomString();

    assertThat(cacheClient.dictionarySetField(cacheName, dictionaryName, "a", "xyz"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldResponse.Success.class);

    assertThat(
            cacheClient.dictionaryIncrement(
                cacheName, dictionaryName, "a", 1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryIncrementResponse.Error.class))
        .satisfies(
            error -> {
              assertThat(error).hasCauseInstanceOf(BadRequestException.class);
              assertThat(error.getErrorCode()).isEqualTo(MomentoErrorCode.BAD_REQUEST_ERROR);
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

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryRemoveFieldReturnsErrorWithNullCacheName() {
    final String dictionaryName = randomString();

    // String field
    assertThat(cacheClient.dictionaryRemoveField(null, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryRemoveFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryRemoveFieldReturnsErrorWithNullDictionaryName() {
    // String field
    assertThat(cacheClient.dictionaryRemoveField(cacheName, null, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryRemoveFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryRemoveFieldReturnsErrorWithNullField() {
    final String dictionaryName = randomString();

    // String field
    assertThat(cacheClient.dictionaryRemoveField(cacheName, dictionaryName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryRemoveFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryRemoveFieldsStringHappyPath() {
    final String dictionaryName = randomString();

    populateTestMaps();
    assertThat(cacheClient.dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "aa")))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryGetFieldsResponse.Miss.class);

    assertThat(
            cacheClient.dictionarySetFields(
                cacheName, dictionaryName, stringStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionarySetFieldsResponse.Success.class);

    assertThat(cacheClient.dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "aa")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryGetFieldsResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, String> stringStringMap = hit.valueMapStringString();
              assertThat(stringStringMap.keySet()).hasSize(2).contains("a", "aa");
              assertThat(stringStringMap.values()).contains("b", "bb");
            });

    assertThat(
            cacheClient.dictionaryRemoveFields(cacheName, dictionaryName, Arrays.asList("a", "aa")))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryRemoveFieldsResponse.Success.class);

    assertThat(cacheClient.dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "aa")))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DictionaryGetFieldsResponse.Miss.class);
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryRemoveFieldsReturnsErrorWithNullCacheName() {
    final String dictionaryName = randomString();

    // String field
    assertThat(
            cacheClient.dictionaryRemoveFields(
                null, dictionaryName, Collections.singletonList("a")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryRemoveFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryRemoveFieldsReturnsErrorWithNullDictionaryName() {
    // String field
    assertThat(cacheClient.dictionaryRemoveFields(cacheName, null, Collections.singletonList("a")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryRemoveFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void dictionaryRemoveFieldsReturnsErrorWithNullField() {
    final String dictionaryName = randomString();

    // String field
    assertThat(cacheClient.dictionaryRemoveFields(cacheName, dictionaryName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryRemoveFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.dictionaryRemoveFields(cacheName, dictionaryName, Arrays.asList("a", null)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DictionaryRemoveFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  private void populateTestMaps() {
    stringStringMap.put("a", "b");
    stringStringMap.put("aa", "bb");

    stringBytesMap.put("c", "d".getBytes());
    stringBytesMap.put("cc", "dd".getBytes());

    bytesStringMap.put("e".getBytes(), "f");
    bytesStringMap.put("ee".getBytes(), "ff");

    bytesBytesMap.put("g".getBytes(), "h".getBytes());
    bytesBytesMap.put("gg".getBytes(), "hh".getBytes());
  }
}
