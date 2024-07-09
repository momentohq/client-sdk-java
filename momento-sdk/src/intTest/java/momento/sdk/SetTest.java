package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.requests.CollectionTtl;
import momento.sdk.responses.cache.set.SetAddElementResponse;
import momento.sdk.responses.cache.set.SetAddElementsResponse;
import momento.sdk.responses.cache.set.SetFetchResponse;
import momento.sdk.responses.cache.set.SetRemoveElementResponse;
import momento.sdk.responses.cache.set.SetRemoveElementsResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class SetTest extends BaseTestClass {
  @Test
  public void setAddElementStringHappyPath() {
    final String setName = randomString();
    final String element1 = "1";
    final String element2 = "2";

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetFetchResponse.Miss.class);

    assertThat(cacheClient.setAddElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element1));

    // Try to add the same element again
    assertThat(
            cacheClient.setAddElement(cacheName, setName, element1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element1));

    // Add a different element
    assertThat(
            cacheClient.setAddElement(cacheName, setName, element2, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueSetString()).hasSize(2).containsOnly(element1, element2));
  }

  @Test
  public void setAddElementBytesHappyPath() {
    final String setName = randomString();
    final byte[] element1 = "one".getBytes();
    final byte[] element2 = "two".getBytes();

    assertThat(cacheClient.setAddElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element1));

    // Try to add the same element again
    assertThat(
            cacheClient.setAddElement(cacheName, setName, element1, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element1));

    // Add a different element
    assertThat(
            cacheClient.setAddElement(cacheName, setName, element2, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsOnly(element1, element2));
  }

  @Test
  public void setAddElementReturnsErrorWithNullCacheName() {
    final String setName = randomString();
    final String elementString = "element";
    final byte[] elementBytes = elementString.getBytes(StandardCharsets.UTF_8);

    assertThat(
            cacheClient.setAddElement(null, setName, elementString, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.setAddElement(null, setName, elementBytes, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementReturnsErrorWithNullSetName() {
    final String elementString = "element";
    final byte[] elementBytes = elementString.getBytes(StandardCharsets.UTF_8);

    assertThat(
            cacheClient.setAddElement(cacheName, null, elementString, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.setAddElement(cacheName, null, elementBytes, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementReturnsErrorWithNullElement() {
    final String setName = randomString();
    assertThat(
            cacheClient.setAddElement(
                cacheName, setName, (String) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.setAddElement(
                cacheName, setName, (byte[]) null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

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
  public void setAddElementsByteArrayHappyPath() {
    final String setName = randomString();
    final Set<byte[]> firstSet = Sets.newHashSet("one".getBytes(), "two".getBytes());
    final Set<byte[]> secondSet = Sets.newHashSet("two".getBytes(), "three".getBytes());

    assertThat(cacheClient.setAddElementsByteArray(cacheName, setName, firstSet))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsAll(firstSet));

    // Try to add the same elements again
    assertThat(
            cacheClient.setAddElementsByteArray(
                cacheName, setName, firstSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsAll(firstSet));

    // Add a set with one new and one overlapping element
    assertThat(
            cacheClient.setAddElementsByteArray(
                cacheName, setName, secondSet, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueSetByteArray())
                    .hasSize(3)
                    .containsAll(firstSet)
                    .containsAll(secondSet));
  }

  @Test
  public void setAddElementsReturnsErrorWithNullCacheName() {
    final String setName = randomString();
    final Set<String> stringElements = Collections.singleton("element");
    final Set<byte[]> bytesElements = Collections.singleton("bytes-element".getBytes());

    assertThat(
            cacheClient.setAddElements(null, setName, stringElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.setAddElementsByteArray(
                null, setName, bytesElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementsReturnsErrorWithNullSetName() {
    final Set<String> stringElements = Collections.singleton("element");
    final Set<byte[]> bytesElements = Collections.singleton("bytes-element".getBytes());

    assertThat(
            cacheClient.setAddElements(
                cacheName, null, stringElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.setAddElementsByteArray(
                cacheName, null, bytesElements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setAddElementsReturnsErrorWithNullElements() {
    final String setName = randomString();
    assertThat(cacheClient.setAddElements(cacheName, cacheName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.setAddElementsByteArray(
                cacheName, cacheName, null, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetAddElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
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

  @Test
  public void setRemoveElementByteArrayHappyPath() {
    final String setName = randomString();
    final byte[] element1 = "one".getBytes();
    final byte[] element2 = "two".getBytes();
    final Set<byte[]> elements = Sets.newHashSet(element1, element2);

    // Add some elements to a set
    assertThat(
            cacheClient.setAddElementsByteArray(
                cacheName, setName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(
            hit -> assertThat(hit.valueSetByteArray()).hasSize(2).containsOnly(element1, element2));

    // Remove an element
    assertThat(cacheClient.setRemoveElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element2));

    // Try to remove the same element again
    assertThat(cacheClient.setRemoveElement(cacheName, setName, element1))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element2));

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

  @Test
  public void setRemoveElementReturnsErrorWithNullCacheName() {
    final String setName = randomString();
    final String elementString = "element";
    final byte[] elementBytes = elementString.getBytes(StandardCharsets.UTF_8);

    assertThat(cacheClient.setRemoveElement(null, setName, elementString))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.setRemoveElement(null, setName, elementBytes))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementReturnsErrorWithNullSetName() {
    final String elementString = "element";
    final byte[] elementBytes = elementString.getBytes(StandardCharsets.UTF_8);

    assertThat(cacheClient.setRemoveElement(cacheName, null, elementString))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.setRemoveElement(cacheName, null, elementBytes))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementReturnsErrorWithNullElement() {
    final String setName = randomString();
    assertThat(cacheClient.setRemoveElement(cacheName, cacheName, (String) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.setRemoveElement(cacheName, cacheName, (byte[]) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementsStringHappyPath() {
    final String setName = randomString();
    final String element1 = "one";
    final String element2 = "two";
    final String element3 = "three";
    final String element4 = "four";
    final Set<String> elements = Sets.newHashSet(element1, element2, element3);

    // Add some elements to a set
    assertThat(
            cacheClient.setAddElements(cacheName, setName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueSetString())
                    .hasSize(3)
                    .containsOnly(element1, element2, element3));

    // Remove some elements that are in the set and one that isn't
    assertThat(
            cacheClient.setRemoveElements(
                cacheName, setName, Sets.newHashSet(element2, element3, element4)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element1));

    // Try to remove an element that has already been removed
    assertThat(cacheClient.setRemoveElements(cacheName, setName, Collections.singleton(element3)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetString()).hasSize(1).containsOnly(element1));

    // Remove everything
    assertThat(cacheClient.setRemoveElements(cacheName, setName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetFetchResponse.Miss.class);

    // Remove elements from the now non-existent set
    assertThat(
            cacheClient.setRemoveElements(
                cacheName, setName, Sets.newHashSet(element1, element2, element3, element4)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetFetchResponse.Miss.class);
  }

  @Test
  public void setRemoveElementsByteArrayHappyPath() {
    final String setName = randomString();
    final byte[] element1 = "one".getBytes();
    final byte[] element2 = "two".getBytes();
    final byte[] element3 = "three".getBytes();
    final byte[] element4 = "four".getBytes();
    final Set<byte[]> elements = Sets.newHashSet(element1, element2, element3);

    // Add some elements to a set
    assertThat(
            cacheClient.setAddElementsByteArray(
                cacheName, setName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetAddElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.valueSetByteArray())
                    .hasSize(3)
                    .containsOnly(element1, element2, element3));

    // Remove some elements that are in the set and one that isn't
    assertThat(
            cacheClient.setRemoveElementsByteArray(
                cacheName, setName, Sets.newHashSet(element2, element3, element4)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element1));

    // Try to remove an element that has already been removed
    assertThat(
            cacheClient.setRemoveElementsByteArray(
                cacheName, setName, Collections.singleton(element3)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.valueSetByteArray()).hasSize(1).containsOnly(element1));

    // Remove everything
    assertThat(cacheClient.setRemoveElementsByteArray(cacheName, setName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetFetchResponse.Miss.class);

    // Remove elements from the now non-existent set
    assertThat(
            cacheClient.setRemoveElementsByteArray(
                cacheName, setName, Sets.newHashSet(element1, element2, element3, element4)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.setFetch(cacheName, setName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SetFetchResponse.Miss.class);
  }

  @Test
  public void setRemoveElementsReturnsErrorWithNullCacheName() {
    final String setName = randomString();
    final Set<String> stringElements = Sets.newHashSet("element");
    final Set<byte[]> bytesElements = Sets.newHashSet("bytes-element".getBytes());

    assertThat(cacheClient.setRemoveElements(null, setName, stringElements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.setRemoveElementsByteArray(null, setName, bytesElements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementsReturnsErrorWithNullSetName() {
    final Set<String> stringElements = Sets.newHashSet("element");
    final Set<byte[]> bytesElements = Sets.newHashSet("bytes-element".getBytes());

    assertThat(cacheClient.setRemoveElements(cacheName, null, stringElements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.setRemoveElementsByteArray(cacheName, null, bytesElements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setRemoveElementsReturnsErrorWithNullElements() {
    final String setName = randomString();
    assertThat(cacheClient.setRemoveElements(cacheName, cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.setRemoveElementsByteArray(cacheName, cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setFetchReturnsErrorWithNullCacheName() {
    final String setName = randomString();
    assertThat(cacheClient.setFetch(null, "set"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void setFetchReturnsErrorWithNullSetName() {
    assertThat(cacheClient.setFetch(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }
}
