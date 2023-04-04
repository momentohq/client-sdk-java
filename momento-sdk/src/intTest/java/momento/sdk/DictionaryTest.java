package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.messages.CacheDictionaryFetchResponse;
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

  AbstractMap.SimpleEntry<String, String> stringStringPair1 =
      new AbstractMap.SimpleEntry<>("a", "b");
  AbstractMap.SimpleEntry<String, String> stringStringPair2 =
      new AbstractMap.SimpleEntry<>("aa", "bb");
  AbstractMap.SimpleEntry<String, byte[]> stringBytesPair1 =
      new AbstractMap.SimpleEntry<>("c", "d".getBytes());
  AbstractMap.SimpleEntry<String, byte[]> stringBytesPair2 =
      new AbstractMap.SimpleEntry<>("cc", "dd".getBytes());
  AbstractMap.SimpleEntry<byte[], String> bytesStringPair1 =
      new AbstractMap.SimpleEntry<>("e".getBytes(), "f");
  AbstractMap.SimpleEntry<byte[], String> bytesStringPair2 =
      new AbstractMap.SimpleEntry<>("ee".getBytes(), "ff");
  AbstractMap.SimpleEntry<byte[], byte[]> bytesBytesPair1 =
      new AbstractMap.SimpleEntry<>("g".getBytes(), "h".getBytes());
  AbstractMap.SimpleEntry<byte[], byte[]> bytesBytesPair2 =
      new AbstractMap.SimpleEntry<>("gg".getBytes(), "hh".getBytes());

  List<AbstractMap.SimpleEntry<String, String>> stringStringPairList =
      Arrays.asList(stringStringPair1, stringStringPair2);
  List<AbstractMap.SimpleEntry<String, byte[]>> stringBytesPairList =
      Arrays.asList(stringBytesPair1, stringBytesPair2);
  List<AbstractMap.SimpleEntry<byte[], String>> bytesStringPairList =
      Arrays.asList(bytesStringPair1, bytesStringPair2);
  List<AbstractMap.SimpleEntry<byte[], byte[]>> bytesBytesPairList =
      Arrays.asList(bytesBytesPair1, bytesBytesPair2);

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
    // Set String key, String value
    assertThat(
            target.dictionarySetFieldsStringString(
                cacheName, dictionaryName, stringStringPairList, CollectionTtl.fromCacheTtl()))
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
                cacheName, dictionaryName, stringBytesPairList, CollectionTtl.fromCacheTtl()))
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
                cacheName, dictionaryName, bytesStringPairList, CollectionTtl.fromCacheTtl()))
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
                cacheName, dictionaryName, bytesBytesPairList, CollectionTtl.fromCacheTtl()))
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
    // Set String key, String value
    assertThat(
            target.dictionarySetFieldsStringString(cacheName, dictionaryName, stringStringPairList))
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
    assertThat(
            target.dictionarySetFieldsStringBytes(cacheName, dictionaryName, stringBytesPairList))
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
            target.dictionarySetFieldsBytesString(cacheName, dictionaryName, bytesStringPairList))
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
    assertThat(target.dictionarySetFieldsBytesBytes(cacheName, dictionaryName, bytesBytesPairList))
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
    // String Key and String value
    assertThat(
            target.dictionarySetFieldsStringString(
                null, dictionaryName, stringStringPairList, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetFieldsStringBytes(
                null, dictionaryName, stringBytesPairList, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and String value
    assertThat(
            target.dictionarySetFieldsBytesString(
                null, dictionaryName, bytesStringPairList, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and Byte value
    assertThat(
            target.dictionarySetFieldsBytesBytes(
                null, dictionaryName, bytesBytesPairList, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionarySetFieldsReturnsErrorWithNullDictionaryName() {
    // String Key and String value
    assertThat(
            target.dictionarySetFieldsStringString(
                cacheName, null, stringStringPairList, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // String Key and Byte value
    assertThat(
            target.dictionarySetFieldsStringBytes(
                cacheName, null, stringBytesPairList, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and String value
    assertThat(
            target.dictionarySetFieldsBytesString(
                cacheName, null, bytesStringPairList, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    // Byte key and Byte value
    assertThat(
            target.dictionarySetFieldsBytesBytes(
                cacheName, null, bytesBytesPairList, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDictionarySetFieldsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void dictionarySetFieldsReturnsErrorWithNullItem() {
    // String Key and String value
    assertThat(
            target.dictionarySetFieldsStringString(
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
}
