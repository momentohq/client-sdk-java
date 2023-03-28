package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.messages.CacheSetAddElementResponse;
import momento.sdk.messages.CacheSetAddElementsResponse;
import momento.sdk.messages.CacheSetFetchResponse;
import momento.sdk.messages.CacheSetRemoveElementResponse;
import momento.sdk.requests.CollectionTtl;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SetTest {
  private static final Duration DEFAULT_TTL = Duration.ofSeconds(60);
  private CacheClient client;
  private String cacheName;
  private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

  private final String setName = "test-set";

  @BeforeEach
  void setup() {
    client = CacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), DEFAULT_TTL).build();
    cacheName = System.getenv("TEST_CACHE_NAME");
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

    assertThat(client.setAddElement(cacheName, setName, element1, CollectionTtl.fromCacheTtl()))
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

    assertThat(client.setAddElement(cacheName, setName, element1, CollectionTtl.fromCacheTtl()))
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
            client.setAddElement(cacheName, cacheName, (String) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.setAddElement(cacheName, cacheName, (byte[]) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddStringElementsHappyPath() {
    final Set<String> firstSet = Sets.newHashSet("one", "two");
    final Set<String> secondSet = Sets.newHashSet("two", "three");

    assertThat(
            client.setAddStringElements(cacheName, setName, firstSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(2).containsAll(firstSet));

    // Try to add the same elements again
    assertThat(
            client.setAddStringElements(cacheName, setName, firstSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(2).containsAll(firstSet));

    // Add a set with one new and one overlapping element
    assertThat(
            client.setAddStringElements(
                cacheName, setName, secondSet, CollectionTtl.fromCacheTtl()))
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
  public void setAddByteArrayElementsHappyPath() {
    final Set<byte[]> firstSet = Sets.newHashSet("one".getBytes(), "two".getBytes());
    final Set<byte[]> secondSet = Sets.newHashSet("two".getBytes(), "three".getBytes());

    assertThat(
            client.setAddByteArrayElements(
                cacheName, setName, firstSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsAll(firstSet));

    // Try to add the same elements again
    assertThat(
            client.setAddByteArrayElements(
                cacheName, setName, firstSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSetAddElementsResponse.Success.class);

    assertThat(client.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsAll(firstSet));

    // Add a set with one new and one overlapping element
    assertThat(
            client.setAddByteArrayElements(
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

    assertThat(
            client.setAddStringElements(
                null, setName, stringElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.setAddByteArrayElements(
                null, setName, bytesElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementsReturnsErrorWithNullSetName() {
    final Set<String> stringElements = Collections.singleton("element");
    final Set<byte[]> bytesElements = Collections.singleton("bytes-element".getBytes());

    assertThat(
            client.setAddStringElements(
                cacheName, null, stringElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.setAddByteArrayElements(
                cacheName, null, bytesElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementsReturnsErrorWithNullElement() {
    assertThat(
            client.setAddStringElements(cacheName, cacheName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.setAddByteArrayElements(
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
    assertThat(
            client.setAddStringElements(cacheName, setName, elements, CollectionTtl.fromCacheTtl()))
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
            client.setAddByteArrayElements(
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
