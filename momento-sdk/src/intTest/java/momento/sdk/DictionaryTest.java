package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.BadRequestException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.messages.CacheDictionaryFetchResponse;
import momento.sdk.messages.CacheDictionaryGetFieldResponse;
import momento.sdk.messages.CacheDictionaryGetFieldsResponse;
import momento.sdk.messages.CacheDictionaryIncrementResponse;
import momento.sdk.messages.CacheDictionaryRemoveFieldResponse;
import momento.sdk.messages.CacheDictionaryRemoveFieldsResponse;
import momento.sdk.messages.CacheDictionarySetFieldResponse;
import momento.sdk.messages.CacheDictionarySetFieldsResponse;
import momento.sdk.requests.CollectionTtl;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DictionaryTest extends BaseTestClass {
  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  private CacheClient target;

  private final String cacheName = System.getenv("TEST_CACHE_NAME");
  private final String dictionaryName = "test-dictionary";

  Map<String, String> stringStringMap = new HashMap<>();
  Map<String, byte[]> stringBytesMap = new HashMap<>();
  Map<byte[], String> bytesStringMap = new HashMap<>();
  Map<byte[], byte[]> bytesBytesMap = new HashMap<>();

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
  public void dictionarySetFieldAndDictionaryFetchAndHappyPath() {
    // Set String key, String Value
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueDictionaryStringString()).hasSize(1).containsEntry("a", "b"));

    // Set String key, ByteArray Value
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "c", "d".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueDictionaryStringBytes())
                    .hasSize(2)
                    .containsEntry("c", "d".getBytes()));
  }

  @Test
  public void dictionarySetFieldAndDictionaryFetchAndHappyPathWithNoTtl() {
    // Set String key, String Value
    assertThat(target.dictionarySetField(cacheName, dictionaryName, "a", "b"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueDictionaryStringString()).hasSize(1).containsEntry("a", "b"));

    // Set String key, ByteArray Value
    assertThat(target.dictionarySetField(cacheName, dictionaryName, "c", "d".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueDictionaryStringBytes())
                    .hasSize(2)
                    .containsEntry("c", "d".getBytes()));
  }

  @Test
  public void dictionarySetFieldReturnsErrorWithNullCacheName() {
    // String Key and String value
    assertThat(
            target.dictionarySetField(null, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetField(
                null, dictionaryName, "a", "b".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionarySetFieldReturnsErrorWithNullDictionaryName() {
    // String Key and String value
    assertThat(target.dictionarySetField(cacheName, null, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetField(
                cacheName, null, "a", "b".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionarySetFieldReturnsErrorWithNullField() {
    // String Key and String value
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, null, "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, null, "b".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionarySetFieldReturnsErrorWithNullValue() {
    // String Key and String value
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "a", (String) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "a", (byte[]) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryFetchReturnsErrorWithNullCacheName() {
    assertThat(target.dictionaryFetch(null, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryFetchReturnsErrorWithNullDictionaryName() {
    assertThat(target.dictionaryFetch(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryFetchReturnsMissWhenDictionaryNotExists() {
    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryFetchResponse.Miss.class);
  }

  @Test
  public void dictionarySetFieldsAndDictionaryFetchAndHappyPath() {
    populateTestMaps();

    // Set String key, String value
    assertThat(
            target.dictionarySetFields(
                cacheName, dictionaryName, stringStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldsResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, String> stringStringMap = hit.valueDictionaryStringString();
              assertThat(stringStringMap.keySet()).hasSize(2).contains("a", "aa");
              assertThat(stringStringMap.values()).contains("bb", "bb");
            });

    // Set String key, byte array value
    assertThat(
            target.dictionarySetFieldsStringBytes(
                cacheName, dictionaryName, stringBytesMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldsResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, byte[]> stringBytesMap = hit.valueDictionaryStringBytes();
              assertThat(stringBytesMap.keySet()).hasSize(4).contains("c", "cc");
              assertThat(stringBytesMap.values()).contains("d".getBytes(), "dd".getBytes());
            });
  }

  @Test
  public void dictionarySetFieldsAndDictionaryFetchAndHappyPathWithNoTtl() {
    populateTestMaps();

    // Set String key, String value
    assertThat(target.dictionarySetFields(cacheName, dictionaryName, stringStringMap))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldsResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, String> stringStringMap = hit.valueDictionaryStringString();
              assertThat(stringStringMap.keySet()).hasSize(2).contains("a", "aa");
              assertThat(stringStringMap.values()).contains("b", "bb");
            });

    // Set String key, byte array value
    assertThat(target.dictionarySetFieldsStringBytes(cacheName, dictionaryName, stringBytesMap))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldsResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, byte[]> stringBytesMap = hit.valueDictionaryStringBytes();
              assertThat(stringBytesMap.keySet()).hasSize(4).contains("c", "cc");
              assertThat(stringBytesMap.values()).contains("d".getBytes(), "dd".getBytes());
            });
  }

  @Test
  public void dictionarySetFieldsReturnsErrorWithNullCacheName() {
    populateTestMaps();

    // String Key and String value
    assertThat(
            target.dictionarySetFields(
                null, dictionaryName, stringStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetFieldsStringBytes(
                null, dictionaryName, stringBytesMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionarySetFieldsReturnsErrorWithNullDictionaryName() {
    populateTestMaps();

    // String Key and String value
    assertThat(
            target.dictionarySetFields(
                cacheName, null, stringStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetFieldsStringBytes(
                cacheName, null, stringBytesMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionarySetFieldsReturnsErrorWithNullItem() {
    // String Key and String value
    assertThat(
            target.dictionarySetFields(
                cacheName, dictionaryName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetFieldsStringBytes(
                cacheName, dictionaryName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldHappyPath() {
    // Get the value as a string
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.fieldString()).isEqualTo("a");
              assertThat(hit.valueString()).isEqualTo("b");
            });

    // Get the value as a byte array
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "c", "d".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryGetField(cacheName, dictionaryName, "c"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.field()).isEqualTo("c");
              assertThat(hit.valueByteArray()).isEqualTo("d".getBytes());
            });
  }

  @Test
  public void dictionaryGetFieldReturnsErrorWithNullCacheName() {
    // String field
    assertThat(target.dictionaryGetField(null, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldReturnsErrorWithNullDictionaryName() {
    // String field
    assertThat(target.dictionaryGetField(cacheName, null, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldReturnsErrorWithNullField() {
    // String field
    assertThat(target.dictionaryGetField(cacheName, dictionaryName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldReturnsMissWhenFieldNotExists() {
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    // String field
    assertThat(target.dictionaryGetField(cacheName, dictionaryName, "c"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryGetFieldResponse.Miss.class);
  }

  @Test
  public void dictionaryGetFieldReturnsMissWhenDictionaryNotExists() {
    // String field
    assertThat(target.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryGetFieldResponse.Miss.class);
  }

  @Test
  public void dictionaryGetFieldsStringHappyPath() {
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "c", "d", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "e", "f", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    // Gets a map of string keys and string values
    assertThat(target.dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "c", "e")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldsResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, String> stringStringMap = hit.valueDictionaryStringString();
              assertThat(stringStringMap.keySet()).hasSize(3).contains("a", "c", "e");
              assertThat(stringStringMap.values()).contains("b", "d", "f");
            });

    // Gets a map of string keys and byte array values
    assertThat(target.dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "c", "e")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldsResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, byte[]> stringStringMap = hit.valueDictionaryStringBytes();
              assertThat(stringStringMap.keySet()).hasSize(3).contains("a", "c", "e");
              assertThat(stringStringMap.values())
                  .contains("b".getBytes(), "d".getBytes(), "f".getBytes());
            });
  }

  @Test
  public void dictionaryGetFieldsReturnsErrorWithNullCacheName() {
    // String fields
    assertThat(target.dictionaryGetFields(null, dictionaryName, Arrays.asList("a", "c", "e")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldsReturnsErrorWithNullDictionaryName() {
    // String fields
    assertThat(target.dictionaryGetFields(cacheName, null, Arrays.asList("a", "c", "e")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldsReturnsErrorWithNullFields() {
    // String fields
    assertThat(target.dictionaryGetFields(cacheName, dictionaryName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    List<String> stringListWithAtleastOneNullField = Arrays.asList("a", null);

    assertThat(
            target.dictionaryGetFields(
                cacheName, dictionaryName, stringListWithAtleastOneNullField))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldsReturnsMissOrHitWhenFieldsNotExistsOrExistsRespectively() {
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "c", "d", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    // get raw responses and validate Hit/Miss
    CacheDictionaryGetFieldsResponse response =
        target.dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "c", "r")).join();

    assertThat(response).isInstanceOf(CacheDictionaryGetFieldsResponse.Hit.class);

    List<CacheDictionaryGetFieldResponse> responseList =
        ((CacheDictionaryGetFieldsResponse.Hit) response).perFieldResponses();
    assertThat(responseList).hasSize(3);

    assertThat(responseList.get(0)).isInstanceOf(CacheDictionaryGetFieldResponse.Hit.class);
    assertThat(((CacheDictionaryGetFieldResponse.Hit) responseList.get(0)).fieldString())
        .isEqualTo("a");
    assertThat(((CacheDictionaryGetFieldResponse.Hit) responseList.get(0)).valueString())
        .isEqualTo("b");
    assertThat(((CacheDictionaryGetFieldResponse.Hit) responseList.get(0)).valueByteArray())
        .isEqualTo("b".getBytes());

    assertThat(((CacheDictionaryGetFieldsResponse.Hit) response).perFieldResponses().get(1))
        .isInstanceOf(CacheDictionaryGetFieldResponse.Hit.class);
    assertThat(((CacheDictionaryGetFieldResponse.Hit) responseList.get(1)).fieldString())
        .isEqualTo("c");
    assertThat(((CacheDictionaryGetFieldResponse.Hit) responseList.get(1)).valueString())
        .isEqualTo("d");
    assertThat(((CacheDictionaryGetFieldResponse.Hit) responseList.get(1)).valueByteArray())
        .isEqualTo("d".getBytes());

    assertThat(((CacheDictionaryGetFieldsResponse.Hit) response).perFieldResponses().get(2))
        .isInstanceOf(CacheDictionaryGetFieldResponse.Miss.class);
    assertThat(((CacheDictionaryGetFieldResponse.Miss) responseList.get(2)).fieldString())
        .isEqualTo("r");
    assertThat(((CacheDictionaryGetFieldResponse.Miss) responseList.get(2)).fieldByteArray())
        .isEqualTo("r".getBytes());
  }

  @Test
  public void dictionaryGetFieldsReturnsMissWhenDictionaryNotExists() {
    // String field
    assertThat(
            target.dictionaryGetFields(cacheName, dictionaryName, Collections.singletonList("a")))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryGetFieldsResponse.Miss.class);
  }

  @Test
  public void dictionaryIncrementStringFieldHappyPath() {
    // Increment with ttl
    assertThat(target.dictionaryIncrement(cacheName, dictionaryName, "a", 1))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.valueNumber()).isEqualTo(1));

    assertThat(
            target.dictionaryIncrement(
                cacheName, dictionaryName, "a", 41, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.valueNumber()).isEqualTo(42));

    // Increment without ttl
    assertThat(
            target.dictionaryIncrement(
                cacheName, dictionaryName, "a", -1042, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.valueNumber()).isEqualTo(-1000));

    assertThat(target.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.fieldString()).isEqualTo("a");
              assertThat(hit.valueString()).isEqualTo("-1000");
            });
  }

  @Test
  public void dictionaryIncrementSetAndResetHappyPath() {
    // Set field
    assertThat(target.dictionarySetField(cacheName, dictionaryName, "a", "10"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(
            target.dictionaryIncrement(
                cacheName, dictionaryName, "a", 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.valueNumber()).isEqualTo(10));

    assertThat(
            target.dictionaryIncrement(
                cacheName, dictionaryName, "a", 90, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.valueNumber()).isEqualTo(100));

    // Reset field
    assertThat(target.dictionarySetField(cacheName, dictionaryName, "a", "0"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(
            target.dictionaryIncrement(
                cacheName, dictionaryName, "a", 0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Success.class))
        .satisfies(success -> assertThat(success.valueNumber()).isEqualTo(0));
  }

  @Test
  public void dictionaryIncrementReturnsErrorWithNullCacheName() {
    // String field
    assertThat(
            target.dictionaryIncrement(null, dictionaryName, "a", 1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryIncrementReturnsErrorWithNullDictionaryName() {
    // String field
    assertThat(target.dictionaryIncrement(cacheName, null, "a", 1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryIncrementReturnsErrorWithNullField() {
    // String field
    assertThat(
            target.dictionaryIncrement(
                cacheName, dictionaryName, null, 1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryIncrementReturnsBadRequestError() {
    assertThat(target.dictionarySetField(cacheName, dictionaryName, "a", "xyz"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(
            target.dictionaryIncrement(
                cacheName, dictionaryName, "a", 1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryIncrementResponse.Error.class))
        .satisfies(
            error -> {
              assertThat(error).hasCauseInstanceOf(BadRequestException.class);
              assertThat(error.getErrorCode()).isEqualTo(MomentoErrorCode.BAD_REQUEST_ERROR);
            });
  }

  @Test
  public void dictionaryRemoveFieldStringHappyPath() {
    assertThat(target.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryGetFieldResponse.Miss.class);

    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "a", "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.fieldString()).isEqualTo("a");
              assertThat(hit.valueString()).isEqualTo("b");
            });

    assertThat(target.dictionaryRemoveField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryRemoveFieldResponse.Success.class);

    assertThat(target.dictionaryGetField(cacheName, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryGetFieldResponse.Miss.class);
  }

  @Test
  public void dictionaryRemoveFieldReturnsErrorWithNullCacheName() {
    // String field
    assertThat(target.dictionaryRemoveField(null, dictionaryName, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryRemoveFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryRemoveFieldReturnsErrorWithNullDictionaryName() {
    // String field
    assertThat(target.dictionaryRemoveField(cacheName, null, "a"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryRemoveFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryRemoveFieldReturnsErrorWithNullField() {
    // String field
    assertThat(target.dictionaryRemoveField(cacheName, dictionaryName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryRemoveFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryRemoveFieldsStringHappyPath() {
    populateTestMaps();
    assertThat(target.dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "aa")))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryGetFieldsResponse.Miss.class);

    assertThat(
            target.dictionarySetFields(
                cacheName, dictionaryName, stringStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldsResponse.Success.class);

    assertThat(target.dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "aa")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldsResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<String, String> stringStringMap = hit.valueDictionaryStringString();
              assertThat(stringStringMap.keySet()).hasSize(2).contains("a", "aa");
              assertThat(stringStringMap.values()).contains("b", "bb");
            });

    assertThat(target.dictionaryRemoveFields(cacheName, dictionaryName, Arrays.asList("a", "aa")))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryRemoveFieldsResponse.Success.class);

    assertThat(target.dictionaryGetFields(cacheName, dictionaryName, Arrays.asList("a", "aa")))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionaryGetFieldsResponse.Miss.class);
  }

  @Test
  public void dictionaryRemoveFieldsReturnsErrorWithNullCacheName() {
    // String field
    assertThat(target.dictionaryRemoveFields(null, dictionaryName, Collections.singletonList("a")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryRemoveFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryRemoveFieldsReturnsErrorWithNullDictionaryName() {
    // String field
    assertThat(target.dictionaryRemoveFields(cacheName, null, Collections.singletonList("a")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryRemoveFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryRemoveFieldsReturnsErrorWithNullField() {
    // String field
    assertThat(target.dictionaryRemoveFields(cacheName, dictionaryName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryRemoveFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(target.dictionaryRemoveFields(cacheName, dictionaryName, Arrays.asList("a", null)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(
            InstanceOfAssertFactories.type(CacheDictionaryRemoveFieldsResponse.Error.class))
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
