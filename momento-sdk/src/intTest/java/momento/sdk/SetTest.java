package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.messages.CacheSetAddElementResponse;
import momento.sdk.messages.CacheSetAddElementsResponse;
import momento.sdk.messages.CacheSetFetchResponse;
import momento.sdk.messages.CacheSetRemoveElementResponse;
import momento.sdk.messages.CacheSetRemoveElementsResponse;
import momento.sdk.requests.CollectionTtl;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SetTest {
  private static final Duration DEFAULT_TTL = Duration.ofSeconds(60);
  private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

  private final CredentialProvider credentialProvider =
      new EnvVarCredentialProvider("TEST_AUTH_TOKEN");
  private final String cacheName = System.getenv("TEST_CACHE_NAME");
  private CacheClient client;

  private final String setName = "test-set";

  @BeforeEach
  void setup() {
    client =
        CacheClient.builder(credentialProvider, Configurations.Laptop.Latest(), DEFAULT_TTL)
            .build();
    client.createCache(cacheName);
  }

  @AfterEach
  void teardown() {
    client.deleteCache(cacheName);
    client.close();
  }

  @Test
  public void setAddElementStringHappyPath() {
    final String element1 = "1";
    final String element2 = "2";

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetFetchResponse.Miss.class);

    assertThat(client.setAddElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element1));

    // Try to add the same element again
    assertThat(client.setAddElement(cacheName, setName, element1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element1));

    // Add a different element
    assertThat(client.setAddElement(cacheName, setName, element2, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueSetString()).hasSize(2).containsOnly(element1, element2));
  }

  @Test
  public void setAddElementBytesHappyPath() {
    final byte[] element1 = "one".getBytes();
    final byte[] element2 = "two".getBytes();

    assertThat(client.setAddElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element1));

    // Try to add the same element again
    assertThat(client.setAddElement(cacheName, setName, element1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element1));

    // Add a different element
    assertThat(client.setAddElement(cacheName, setName, element2, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsOnly(element1, element2));
  }

  @Test
  public void setAddElementReturnsErrorWithNullCacheName() {
    final String elementString = "element";
    final byte[] elementBytes = elementString.getBytes(StandardCharsets.UTF_8);

    assertThat(client.setAddElement(null, setName, elementString, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.setAddElement(null, setName, elementBytes, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementReturnsErrorWithNullSetName() {
    final String elementString = "element";
    final byte[] elementBytes = elementString.getBytes(StandardCharsets.UTF_8);

    assertThat(client.setAddElement(cacheName, null, elementString, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.setAddElement(cacheName, null, elementBytes, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementReturnsErrorWithNullElement() {
    assertThat(
            client.setAddElement(cacheName, setName, (String) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.setAddElement(cacheName, setName, (byte[]) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementsStringHappyPath() {
    final Set<String> firstSet = Sets.newHashSet("one", "two");
    final Set<String> secondSet = Sets.newHashSet("two", "three");

    assertThat(client.setAddElements(cacheName, setName, firstSet))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(2).containsAll(firstSet));

    // Try to add the same elements again
    assertThat(client.setAddElements(cacheName, setName, firstSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(2).containsAll(firstSet));

    // Add a set with one new and one overlapping element
    assertThat(client.setAddElements(cacheName, setName, secondSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueSetString())
                    .hasSize(3)
                    .containsAll(firstSet)
                    .containsAll(secondSet));
  }

  @Test
  public void setAddElementsByteArrayHappyPath() {
    final Set<byte[]> firstSet = Sets.newHashSet("one".getBytes(), "two".getBytes());
    final Set<byte[]> secondSet = Sets.newHashSet("two".getBytes(), "three".getBytes());

    assertThat(client.setAddElementsByteArray(cacheName, setName, firstSet))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsAll(firstSet));

    // Try to add the same elements again
    assertThat(
            client.setAddElementsByteArray(
                cacheName, setName, firstSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsAll(firstSet));

    // Add a set with one new and one overlapping element
    assertThat(
            client.setAddElementsByteArray(
                cacheName, setName, secondSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueSetByteArray())
                    .hasSize(3)
                    .containsAll(firstSet)
                    .containsAll(secondSet));
  }

  @Test
  public void setAddElementsReturnsErrorWithNullCacheName() {
    final Set<String> stringElements = Collections.singleton("element");
    final Set<byte[]> bytesElements = Collections.singleton("bytes-element".getBytes());

    assertThat(client.setAddElements(null, setName, stringElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.setAddElementsByteArray(
                null, setName, bytesElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementsReturnsErrorWithNullSetName() {
    final Set<String> stringElements = Collections.singleton("element");
    final Set<byte[]> bytesElements = Collections.singleton("bytes-element".getBytes());

    assertThat(client.setAddElements(cacheName, null, stringElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.setAddElementsByteArray(
                cacheName, null, bytesElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementsReturnsErrorWithNullElements() {
    assertThat(client.setAddElements(cacheName, cacheName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.setAddElementsByteArray(
                cacheName, cacheName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementStringHappyPath() {
    final String element1 = "one";
    final String element2 = "two";
    final Set<String> elements = Sets.newHashSet(element1, element2);

    // Add some elements to a set
    assertThat(client.setAddElements(cacheName, setName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueSetString()).hasSize(2).containsOnly(element1, element2));

    // Remove an element
    assertThat(client.setRemoveElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element2));

    // Try to remove the same element again
    assertThat(client.setRemoveElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element2));

    // Remove the last element
    assertThat(client.setRemoveElement(cacheName, setName, element2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetFetchResponse.Miss.class);

    // Remove an element from the now non-existent set
    assertThat(client.setRemoveElement(cacheName, setName, element2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetFetchResponse.Miss.class);
  }

  @Test
  public void setRemoveElementByteArrayHappyPath() {
    final byte[] element1 = "one".getBytes();
    final byte[] element2 = "two".getBytes();
    final Set<byte[]> elements = Sets.newHashSet(element1, element2);

    // Add some elements to a set
    assertThat(
            client.setAddElementsByteArray(
                cacheName, setName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsOnly(element1, element2));

    // Remove an element
    assertThat(client.setRemoveElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element2));

    // Try to remove the same element again
    assertThat(client.setRemoveElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element2));

    // Remove the last element
    assertThat(client.setRemoveElement(cacheName, setName, element2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetFetchResponse.Miss.class);

    // Remove an element from the now non-existent set
    assertThat(client.setRemoveElement(cacheName, setName, element2))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetFetchResponse.Miss.class);
  }

  @Test
  public void setRemoveElementReturnsErrorWithNullCacheName() {
    final String elementString = "element";
    final byte[] elementBytes = elementString.getBytes(StandardCharsets.UTF_8);

    assertThat(client.setRemoveElement(null, setName, elementString))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.setRemoveElement(null, setName, elementBytes))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementReturnsErrorWithNullSetName() {
    final String elementString = "element";
    final byte[] elementBytes = elementString.getBytes(StandardCharsets.UTF_8);

    assertThat(client.setRemoveElement(cacheName, null, elementString))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.setRemoveElement(cacheName, null, elementBytes))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementReturnsErrorWithNullElement() {
    assertThat(client.setRemoveElement(cacheName, cacheName, (String) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.setRemoveElement(cacheName, cacheName, (byte[]) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementsStringHappyPath() {
    final String element1 = "one";
    final String element2 = "two";
    final String element3 = "three";
    final String element4 = "four";
    final Set<String> elements = Sets.newHashSet(element1, element2, element3);

    // Add some elements to a set
    assertThat(client.setAddElements(cacheName, setName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueSetString())
                    .hasSize(3)
                    .containsOnly(element1, element2, element3));

    // Remove some elements that are in the set and one that isn't
    assertThat(
            client.setRemoveElements(
                cacheName, setName, Sets.newHashSet(element2, element3, element4)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element1));

    // Try to remove an element that has already been removed
    assertThat(client.setRemoveElements(cacheName, setName, Collections.singleton(element3)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element1));

    // Remove everything
    assertThat(client.setRemoveElements(cacheName, setName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetFetchResponse.Miss.class);

    // Remove elements from the now non-existent set
    assertThat(
            client.setRemoveElements(
                cacheName, setName, Sets.newHashSet(element1, element2, element3, element4)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetFetchResponse.Miss.class);
  }

  @Test
  public void setRemoveElementsByteArrayHappyPath() {
    final byte[] element1 = "one".getBytes();
    final byte[] element2 = "two".getBytes();
    final byte[] element3 = "three".getBytes();
    final byte[] element4 = "four".getBytes();
    final Set<byte[]> elements = Sets.newHashSet(element1, element2, element3);

    // Add some elements to a set
    assertThat(
            client.setAddElementsByteArray(
                cacheName, setName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueSetByteArray())
                    .hasSize(3)
                    .containsOnly(element1, element2, element3));

    // Remove some elements that are in the set and one that isn't
    assertThat(
            client.setRemoveElementsByteArray(
                cacheName, setName, Sets.newHashSet(element2, element3, element4)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element1));

    // Try to remove an element that has already been removed
    assertThat(
            client.setRemoveElementsByteArray(cacheName, setName, Collections.singleton(element3)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element1));

    // Remove everything
    assertThat(client.setRemoveElementsByteArray(cacheName, setName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetFetchResponse.Miss.class);

    // Remove elements from the now non-existent set
    assertThat(
            client.setRemoveElementsByteArray(
                cacheName, setName, Sets.newHashSet(element1, element2, element3, element4)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetRemoveElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetFetchResponse.Miss.class);
  }

  @Test
  public void setRemoveElementsReturnsErrorWithNullCacheName() {
    final Set<String> stringElements = Sets.newHashSet("element");
    final Set<byte[]> bytesElements = Sets.newHashSet("bytes-element".getBytes());

    assertThat(client.setRemoveElements(null, setName, stringElements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.setRemoveElementsByteArray(null, setName, bytesElements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementsReturnsErrorWithNullSetName() {
    final Set<String> stringElements = Sets.newHashSet("element");
    final Set<byte[]> bytesElements = Sets.newHashSet("bytes-element".getBytes());

    assertThat(client.setRemoveElements(cacheName, null, stringElements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.setRemoveElementsByteArray(cacheName, null, bytesElements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementsReturnsErrorWithNullElements() {
    assertThat(client.setRemoveElements(cacheName, cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.setRemoveElementsByteArray(cacheName, cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setFetchReturnsErrorWithNullCacheName() {
    assertThat(client.setFetch(null, "set"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setFetchReturnsErrorWithNullSetName() {
    assertThat(client.setFetch(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }
}
