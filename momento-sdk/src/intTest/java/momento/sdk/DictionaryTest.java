package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.messages.CacheDictionaryFetchResponse;
import momento.sdk.messages.CacheDictionaryGetFieldResponse;
import momento.sdk.messages.CacheDictionarySetFieldResponse;
import momento.sdk.messages.CacheDictionarySetFieldsResponse;
import momento.sdk.requests.CollectionTtl;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DictionaryTest extends BaseTestClass {
  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
  private CacheClient target;

  private final CredentialProvider credentialProvider =
      new EnvVarCredentialProvider("TEST_AUTH_TOKEN");
  private final String cacheName = System.getenv("TEST_CACHE_NAME");
  private final String dictionaryName = "test-dictionary";

  Map<String, String> stringStringMap = new HashMap<>();
  Map<String, byte[]> stringBytesMap = new HashMap<>();
  Map<byte[], String> bytesStringMap = new HashMap<>();
  Map<byte[], byte[]> bytesBytesMap = new HashMap<>();

  @BeforeEach
  void setup() {
    target =
        CacheClient.builder(credentialProvider, Configurations.Laptop.Latest(), DEFAULT_TTL_SECONDS)
            .build();
    target.createCache(cacheName);
  }

  @AfterEach
  void teardown() {
    target.deleteCache(cacheName);
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

    // Set ByteArray key, String Value
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "e".getBytes(), "f", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<byte[], String> bytesStringMap = hit.valueDictionaryBytesString();
              assertThat(bytesStringMap.keySet()).hasSize(3).contains("e".getBytes());
              assertThat(bytesStringMap.values()).contains("f");
            });

    //     Set ByteArray key, ByteArray Value
    assertThat(
            target.dictionarySetField(
                cacheName,
                dictionaryName,
                "g".getBytes(),
                "h".getBytes(),
                CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<byte[], byte[]> byetByteMap = hit.valueDictionaryBytesBytes();
              assertThat(byetByteMap.keySet()).hasSize(4).contains("g".getBytes());
              assertThat(byetByteMap.values()).contains("h".getBytes());
            });
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

    // Set ByteArray key, String Value
    assertThat(target.dictionarySetField(cacheName, dictionaryName, "e".getBytes(), "f"))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<byte[], String> bytesStringMap = hit.valueDictionaryBytesString();
              assertThat(bytesStringMap.keySet()).hasSize(3).contains("e".getBytes());
              assertThat(bytesStringMap.values()).contains("f");
            });

    //     Set ByteArray key, ByteArray Value
    assertThat(target.dictionarySetField(cacheName, dictionaryName, "g".getBytes(), "h".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<byte[], byte[]> byetByteMap = hit.valueDictionaryBytesBytes();
              assertThat(byetByteMap.keySet()).hasSize(4).contains("g".getBytes());
              assertThat(byetByteMap.values()).contains("h".getBytes());
            });
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

    // Byte key and String value
    assertThat(
            target.dictionarySetField(
                null, dictionaryName, "a".getBytes(), "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and Byte value
    assertThat(
            target.dictionarySetField(
                null, dictionaryName, "a".getBytes(), "b".getBytes(), CollectionTtl.fromCacheTtl()))
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

    // Byte key and String value
    assertThat(
            target.dictionarySetField(
                cacheName, null, "a".getBytes(), "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and Byte value
    assertThat(
            target.dictionarySetField(
                cacheName, null, "a".getBytes(), "b".getBytes(), CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionarySetFieldReturnsErrorWithNullField() {
    // String Key and String value
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, (String) null, "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetField(
                cacheName,
                dictionaryName,
                (String) null,
                "b".getBytes(),
                CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and String value
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, (byte[]) null, "b", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and Byte value
    assertThat(
            target.dictionarySetField(
                cacheName,
                dictionaryName,
                (byte[]) null,
                "b".getBytes(),
                CollectionTtl.fromCacheTtl()))
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

    // Byte key and String value
    assertThat(
            target.dictionarySetField(
                cacheName, null, "a".getBytes(), (String) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and Byte value
    assertThat(
            target.dictionarySetField(
                cacheName,
                dictionaryName,
                "a".getBytes(),
                (byte[]) null,
                CollectionTtl.fromCacheTtl()))
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

    // Set byte array key, String value
    assertThat(
            target.dictionarySetFieldsBytesString(
                cacheName, dictionaryName, bytesStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldsResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<byte[], String> bytesStringMap = hit.valueDictionaryBytesString();
              assertThat(bytesStringMap.keySet())
                  .hasSize(6)
                  .contains("e".getBytes(), "ee".getBytes());
              assertThat(bytesStringMap.values()).contains("f", "ff");
            });

    // Set byte array key, byte array value
    assertThat(
            target.dictionarySetFieldsBytesBytes(
                cacheName, dictionaryName, bytesBytesMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldsResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<byte[], byte[]> bytesStringMap = hit.valueDictionaryBytesBytes();
              assertThat(bytesStringMap.keySet())
                  .hasSize(8)
                  .contains("g".getBytes(), "gg".getBytes());
              assertThat(bytesStringMap.values()).contains("h".getBytes(), "hh".getBytes());
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

    // Set byte array key, String value
    assertThat(target.dictionarySetFieldsBytesString(cacheName, dictionaryName, bytesStringMap))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldsResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<byte[], String> bytesStringMap = hit.valueDictionaryBytesString();
              assertThat(bytesStringMap.keySet())
                  .hasSize(6)
                  .contains("e".getBytes(), "ee".getBytes());
              assertThat(bytesStringMap.values()).contains("f", "ff");
            });

    // Set byte array key, byte array value
    assertThat(target.dictionarySetFieldsBytesBytes(cacheName, dictionaryName, bytesBytesMap))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldsResponse.Success.class);

    assertThat(target.dictionaryFetch(cacheName, dictionaryName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final Map<byte[], byte[]> bytesStringMap = hit.valueDictionaryBytesBytes();
              assertThat(bytesStringMap.keySet())
                  .hasSize(8)
                  .contains("g".getBytes(), "gg".getBytes());
              assertThat(bytesStringMap.values()).contains("h".getBytes(), "hh".getBytes());
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

    // Byte key and String value
    assertThat(
            target.dictionarySetFieldsBytesString(
                null, dictionaryName, bytesStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and Byte value
    assertThat(
            target.dictionarySetFieldsBytesBytes(
                null, dictionaryName, bytesBytesMap, CollectionTtl.fromCacheTtl()))
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

    // Byte key and String value
    assertThat(
            target.dictionarySetFieldsBytesString(
                cacheName, null, bytesStringMap, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and Byte value
    assertThat(
            target.dictionarySetFieldsBytesBytes(
                cacheName, null, bytesBytesMap, CollectionTtl.fromCacheTtl()))
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

    // Byte key and String value
    assertThat(
            target.dictionarySetFieldsBytesString(
                cacheName, dictionaryName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and Byte value
    assertThat(
            target.dictionarySetFieldsBytesBytes(
                cacheName, dictionaryName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldHappyPath() {
    // Get field/value as string
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

    // Get field/value as byte array
    assertThat(
            target.dictionarySetField(
                cacheName, dictionaryName, "c", "d", CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDictionarySetFieldResponse.Success.class);

    assertThat(target.dictionaryGetField(cacheName, dictionaryName, "c".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.fieldByteArray()).isEqualTo("c".getBytes());
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

    // Byte Array field
    assertThat(target.dictionaryGetField(null, dictionaryName, "a".getBytes()))
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

    // Byte Array field
    assertThat(target.dictionaryGetField(cacheName, null, "a".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionaryGetFieldReturnsErrorWithNullField() {
    // String field
    assertThat(target.dictionaryGetField(cacheName, dictionaryName, (String) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte Array field
    assertThat(target.dictionaryGetField(cacheName, dictionaryName, (byte[]) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionaryGetFieldResponse.Error.class))
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
