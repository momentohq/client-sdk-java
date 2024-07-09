package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.requests.CollectionTtl;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.cache.sortedset.ScoredElement;
import momento.sdk.responses.cache.sortedset.SortedSetFetchResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetRankResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetScoreResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetScoresResponse;
import momento.sdk.responses.cache.sortedset.SortedSetIncrementScoreResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementsResponse;
import momento.sdk.responses.cache.sortedset.SortedSetRemoveElementResponse;
import momento.sdk.responses.cache.sortedset.SortedSetRemoveElementsResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class SortedSetTest extends BaseTestClass {
  // sortedSetPutElement

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
  public void sortedSetPutElementBytesHappyPath() {
    final String sortedSetName = randomString();
    final byte[] value = "1".getBytes();
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
              assertThat(scoredElements)
                  .map(ScoredElement::getElementByteArray)
                  .containsOnly(value);
              assertThat(scoredElements).map(ScoredElement::getScore).containsOnly(score);
            });
  }

  @Test
  public void sortedSetPutElementsWithScoredElementsHappyPath() {
    final String sortedSetName = randomString();
    final String one = "1";
    final String two = "2";
    final String three = "3";
    final String four = "4";
    final String five = "5";

    final List<ScoredElement> elements = new ArrayList<>();
    elements.add(new ScoredElement(one, 0.0));
    elements.add(new ScoredElement(two, 1.0));
    elements.add(new ScoredElement(three, 0.5));
    elements.add(new ScoredElement(four, 2.0));
    elements.add(new ScoredElement(five, 1.5));

    assertThat(
            cacheClient.sortedSetPutElements(
                cacheName, sortedSetName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

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
  public void sortedSetPutElementReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetPutElement(null, sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetPutElement(null, sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetPutElementReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetPutElement(randomString("cache"), sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));

    assertThat(
            cacheClient.sortedSetPutElement(
                randomString("cache"), sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetPutElementReturnsErrorWithNullSetName() {
    assertThat(cacheClient.sortedSetPutElement(cacheName, null, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetPutElement(cacheName, null, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetPutElements

  @Test
  public void sortedSetPutElementsStringHappyPath() {
    final String sortedSetName = randomString();
    final Map<String, Double> elements = new HashMap<>();
    elements.put("1", 0.1);
    elements.put("2", 0.5);
    elements.put("3", 1.0);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(
            cacheClient.sortedSetPutElements(
                cacheName, sortedSetName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements)
                  .map(ScoredElement::getValue)
                  .containsAll(elements.keySet());
              assertThat(scoredElements)
                  .map(ScoredElement::getScore)
                  .containsAll(elements.values());
            });
  }

  @Test
  public void sortedSetPutElementsBytesHappyPath() {
    final String sortedSetName = randomString();
    final Map<byte[], Double> elements = new HashMap<>();
    elements.put("1".getBytes(), 0.0);
    elements.put("2".getBytes(), 0.5);
    elements.put("3".getBytes(), 1.0);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(
            cacheClient.sortedSetPutElementsByteArray(
                cacheName, sortedSetName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements)
                  .map(ScoredElement::getElementByteArray)
                  .containsAll(elements.keySet());
              assertThat(scoredElements)
                  .map(ScoredElement::getScore)
                  .containsAll(elements.values());
            });
  }

  @Test
  public void sortedSetPutElementsReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetPutElements(
                null, sortedSetName, Collections.singletonMap("element", 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetPutElementsByteArray(
                null, sortedSetName, Collections.singletonMap("element".getBytes(), 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetPutElementsReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetPutElements(
                randomString("cache"), sortedSetName, Collections.singletonMap("element", 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));

    assertThat(
            cacheClient.sortedSetPutElementsByteArray(
                randomString("cache"),
                sortedSetName,
                Collections.singletonMap("element".getBytes(), 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetPutElementsReturnsErrorWithNullSetName() {
    assertThat(
            cacheClient.sortedSetPutElements(
                cacheName, null, Collections.singletonMap("element", 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetPutElementsByteArray(
                cacheName, null, Collections.singletonMap("element".getBytes(), 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetFetchByRank

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
  public void sortedSetFetchByRankBytesHappyPath() {
    final String sortedSetName = randomString();
    final byte[] one = "1".getBytes();
    final byte[] two = "2".getBytes();
    final byte[] three = "3".getBytes();
    final byte[] four = "4".getBytes();
    final byte[] five = "5".getBytes();

    final Map<byte[], Double> elements = new HashMap<>();
    elements.put(one, 0.0);
    elements.put(two, 1.0);
    elements.put(three, 0.5);
    elements.put(four, 2.0);
    elements.put(five, 1.5);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(cacheClient.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements))
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
                  .map(ScoredElement::getElementByteArray)
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
                  .map(ScoredElement::getElementByteArray)
                  .containsSequence(five, two, three);
            });
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithInvalidIndexRange() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetFetchByRank(
                cacheName, sortedSetName, 1000, -5, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetFetchByRank(null, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetFetchByRank(randomString("cache"), sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithNullSetName() {
    assertThat(cacheClient.sortedSetFetchByRank(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetFetchByScore

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
  public void sortedSetFetchByScoreReturnsErrorWithInvalidScoreRange() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetFetchByScore(
                null, sortedSetName, 10.0, 0.5, SortOrder.ASCENDING, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetFetchByScore(null, sortedSetName, null, null, null, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetFetchByScore(
                randomString("cache"), sortedSetName, null, null, null, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithNullSetName() {
    assertThat(cacheClient.sortedSetFetchByScore(cacheName, null, null, null, null, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetGetRank

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
  public void sortedSetGetRankReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetGetRank(null, sortedSetName, "element", SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetGetRank(
                null, sortedSetName, "element".getBytes(), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetRankReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetGetRank(
                randomString("cache"), sortedSetName, "element", SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));

    assertThat(
            cacheClient.sortedSetGetRank(
                randomString("cache"), sortedSetName, "element".getBytes(), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetGetRankReturnsErrorWithNullSetName() {
    assertThat(cacheClient.sortedSetGetRank(cacheName, null, "element", SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetGetRank(
                cacheName, null, "element".getBytes(), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetRankReturnsErrorWithNullElement() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetGetRank(
                cacheName, sortedSetName, (String) null, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetGetRank(
                cacheName, sortedSetName, (byte[]) null, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetGetScore

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
  public void sortedSetGetScoreBytesHappyPath() {
    final String sortedSetName = randomString();
    final byte[] one = "1".getBytes();
    final byte[] two = "2".getBytes();

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
  public void sortedSetGetScoreReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetGetScore(null, sortedSetName, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetGetScore(null, sortedSetName, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetScoreReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetGetScore(randomString("cache"), sortedSetName, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));

    assertThat(
            cacheClient.sortedSetGetScore(
                randomString("cache"), sortedSetName, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetGetScoreReturnsErrorWithNullSetName() {
    assertThat(cacheClient.sortedSetGetScore(cacheName, null, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetGetScore(cacheName, null, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetScoreReturnsErrorWithNullElement() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetGetScore(cacheName, sortedSetName, (String) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetGetScore(cacheName, sortedSetName, (byte[]) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetGetScores

  @Test
  public void sortedSetGetScoresStringHappyPath() {
    final String sortedSetName = randomString();
    final String one = "1";
    final String two = "2";
    final Set<String> elements = new HashSet<>();
    elements.add(one);
    elements.add(two);

    assertThat(cacheClient.sortedSetGetScores(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetGetScoresResponse.Miss.class);

    assertThat(cacheClient.sortedSetPutElement(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    // One element in the set, one not in the set
    assertThat(cacheClient.sortedSetGetScores(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.scoredElements())
                  .filteredOn(se -> elements.contains(se.getValue()))
                  .hasSize(1)
                  .map(ScoredElement::getScore)
                  .containsOnly(1.0);
            });

    // Add the other element
    assertThat(cacheClient.sortedSetPutElement(cacheName, sortedSetName, two, 0.5))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(cacheClient.sortedSetGetScores(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Hit.class))
        .satisfies(
            hit -> {
              assertThat(hit.scoredElements())
                  .filteredOn(se -> elements.contains(se.getValue()))
                  .hasSize(2)
                  .map(ScoredElement::getScore)
                  .containsOnly(1.0, 0.5);
            });
  }

  @Test
  public void sortedSetGetScoresReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetGetScores(null, sortedSetName, Collections.singleton("element")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetGetScoresByteArray(
                null, sortedSetName, Collections.singleton("element".getBytes())))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetScoresReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetGetScores(
                randomString("cache"), sortedSetName, Collections.singleton("element")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));

    assertThat(
            cacheClient.sortedSetGetScoresByteArray(
                randomString("cache"), sortedSetName, Collections.singleton("element".getBytes())))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetGetScoresReturnsErrorWithNullSetName() {
    assertThat(cacheClient.sortedSetGetScores(cacheName, null, Collections.singleton("element")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetGetScoresByteArray(
                cacheName, null, Collections.singleton("element".getBytes())))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetScoresReturnsErrorWithNullElements() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetGetScores(cacheName, sortedSetName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetGetScoresByteArray(cacheName, sortedSetName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetIncrementScore

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
  public void sortedSetIncrementScoreBytesHappyPath() {
    final String sortedSetName = randomString();
    final byte[] one = "1".getBytes();

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
  public void sortedSetIncrementScoreReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetIncrementScore(null, sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetIncrementScore(null, sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetIncrementScoreReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetIncrementScore(
                randomString("cache"), sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));

    assertThat(
            cacheClient.sortedSetIncrementScore(
                randomString("cache"), sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetIncrementScoreReturnsErrorWithNullSetName() {
    assertThat(cacheClient.sortedSetIncrementScore(cacheName, null, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetIncrementScore(cacheName, null, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetIncrementScoreReturnsErrorWithNullElement() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetIncrementScore(
                cacheName, sortedSetName, (String) null, 1.0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetIncrementScore(
                cacheName, sortedSetName, (byte[]) null, 1.0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetRemoveElement

  @Test
  public void sortedSetRemoveElementStringHappyPath() {
    final String sortedSetName = randomString();
    final String one = "1";
    final String two = "2";
    final String three = "3";
    final Map<String, Double> elements = ImmutableMap.of(one, 1.0, two, 2.0, three, 3.0);

    assertThat(cacheClient.sortedSetRemoveElement(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementResponse.Success.class);

    assertThat(cacheClient.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(cacheClient.sortedSetRemoveElement(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementResponse.Success.class);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.elementsList())
                    .hasSize(2)
                    .map(ScoredElement::getValue)
                    .containsOnly(two, three));
  }

  @Test
  public void sortedSetRemoveElementBytesHappyPath() {
    final String sortedSetName = randomString();
    final byte[] one = "1".getBytes();
    final byte[] two = "2".getBytes();
    final byte[] three = "3".getBytes();
    final Map<byte[], Double> elements = ImmutableMap.of(one, 1.0, two, 2.0, three, 3.0);

    assertThat(cacheClient.sortedSetRemoveElement(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementResponse.Success.class);

    assertThat(cacheClient.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(cacheClient.sortedSetRemoveElement(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementResponse.Success.class);

    assertThat(cacheClient.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.elementsList())
                    .hasSize(2)
                    .map(ScoredElement::getElementByteArray)
                    .containsOnly(two, three));
  }

  @Test
  public void sortedSetRemoveElementReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetRemoveElement(null, sortedSetName, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetRemoveElement(null, sortedSetName, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetRemoveElementReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetRemoveElement(randomString("cache"), sortedSetName, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));

    assertThat(
            cacheClient.sortedSetRemoveElement(
                randomString("cache"), sortedSetName, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetRemoveElementReturnsErrorWithNullSetName() {
    assertThat(cacheClient.sortedSetRemoveElement(cacheName, null, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetRemoveElement(cacheName, null, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetRemoveElementReturnsErrorWithNullElement() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetRemoveElement(cacheName, sortedSetName, (String) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetRemoveElement(cacheName, sortedSetName, (byte[]) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetRemoveElements

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

  @Test
  public void sortedSetRemoveElementsBytesHappyPath() {
    final String sortedSetName = randomString();
    final byte[] one = "1".getBytes();
    final byte[] two = "2".getBytes();
    final byte[] three = "3".getBytes();
    final Map<byte[], Double> elements = ImmutableMap.of(one, 1.0, two, 2.0, three, 3.0);

    assertThat(
            cacheClient.sortedSetRemoveElementsByteArray(
                cacheName, sortedSetName, elements.keySet()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementsResponse.Success.class);

    assertThat(cacheClient.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(
            cacheClient.sortedSetRemoveElementsByteArray(
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
                    .map(ScoredElement::getElementByteArray)
                    .containsOnly(three));
  }

  @Test
  public void sortedSetRemoveElementsReturnsErrorWithNullCacheName() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetRemoveElements(null, sortedSetName, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetRemoveElementsByteArray(
                null, sortedSetName, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetRemoveElementsReturnsErrorWithNonexistentCacheName() {
    final String sortedSetName = randomString();
    assertThat(
            cacheClient.sortedSetRemoveElements(
                randomString("cache"), sortedSetName, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));

    assertThat(
            cacheClient.sortedSetRemoveElementsByteArray(
                randomString("cache"), sortedSetName, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void sortedSetRemoveElementsReturnsErrorWithNullSetName() {
    assertThat(cacheClient.sortedSetRemoveElements(cacheName, null, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            cacheClient.sortedSetRemoveElementsByteArray(cacheName, null, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetRemoveElementsReturnsErrorWithNullElements() {
    final String sortedSetName = randomString();
    assertThat(cacheClient.sortedSetRemoveElements(cacheName, sortedSetName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(cacheClient.sortedSetRemoveElementsByteArray(cacheName, sortedSetName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }
}
