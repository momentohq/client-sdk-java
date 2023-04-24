package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SortedSetTest extends BaseTestClass {
  private static final Duration DEFAULT_TTL = Duration.ofSeconds(60);

  private final String cacheName = System.getenv("TEST_CACHE_NAME");
  private CacheClient client;

  private String sortedSetName;

  @BeforeEach
  void setup() {
    client =
        CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL)
            .build();
    client.createCache(cacheName).join();
    sortedSetName = randomString("sortedSet");
  }

  @AfterEach
  void teardown() {
    client.deleteCache(cacheName).join();
    client.close();
  }

  // sortedSetPutElement

  @Test
  public void sortedSetPutElementStringHappyPath() {
    final String value = "1";
    final double score = 1.0;

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(
            client.sortedSetPutElement(
                cacheName, sortedSetName, value, score, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
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
    final byte[] value = "1".getBytes();
    final double score = 1.0;

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(
            client.sortedSetPutElement(
                cacheName, sortedSetName, value, score, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
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
            client.sortedSetPutElements(
                cacheName, sortedSetName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    // Full set ascending
    assertThat(client.sortedSetFetchByScore(cacheName, sortedSetName))
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
    assertThat(client.sortedSetPutElement(null, sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetPutElement(null, sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetPutElementReturnsErrorWithNonexistentCacheName() {
    assertThat(client.sortedSetPutElement(randomString("cache"), sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(
            client.sortedSetPutElement(
                randomString("cache"), sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetPutElementReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetPutElement(cacheName, null, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetPutElement(cacheName, null, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetPutElements

  @Test
  public void sortedSetPutElementsStringHappyPath() {
    final Map<String, Double> elements = new HashMap<>();
    elements.put("1", 0.1);
    elements.put("2", 0.5);
    elements.put("3", 1.0);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(
            client.sortedSetPutElements(
                cacheName, sortedSetName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
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
    final Map<byte[], Double> elements = new HashMap<>();
    elements.put("1".getBytes(), 0.0);
    elements.put("2".getBytes(), 0.5);
    elements.put("3".getBytes(), 1.0);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(
            client.sortedSetPutElementsByteArray(
                cacheName, sortedSetName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
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
    assertThat(
            client.sortedSetPutElements(
                null, sortedSetName, Collections.singletonMap("element", 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.sortedSetPutElementsByteArray(
                null, sortedSetName, Collections.singletonMap("element".getBytes(), 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetPutElementsReturnsErrorWithNonexistentCacheName() {
    assertThat(
            client.sortedSetPutElements(
                randomString("cache"), sortedSetName, Collections.singletonMap("element", 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(
            client.sortedSetPutElementsByteArray(
                randomString("cache"),
                sortedSetName,
                Collections.singletonMap("element".getBytes(), 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetPutElementsReturnsErrorWithNullSetName() {
    assertThat(
            client.sortedSetPutElements(cacheName, null, Collections.singletonMap("element", 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.sortedSetPutElementsByteArray(
                cacheName, null, Collections.singletonMap("element".getBytes(), 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetFetchByRank

  @Test
  public void sortedSetFetchByRankStringHappyPath() {
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

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(client.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    // Full set ascending, end index larger than set
    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName, 0, 6, SortOrder.ASCENDING))
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
    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName, 1, 4, SortOrder.DESCENDING))
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

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(client.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    // Full set ascending, end index larger than set
    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName, 0, 6, SortOrder.ASCENDING))
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
    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName, 1, 4, SortOrder.DESCENDING))
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
    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName, 1000, -5, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithNullCacheName() {
    assertThat(client.sortedSetFetchByRank(null, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithNonexistentCacheName() {
    assertThat(client.sortedSetFetchByRank(randomString("cache"), sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetFetchByRank(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetFetchByScore

  @Test
  public void sortedSetFetchByScoreStringHappyPath() {
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

    assertThat(client.sortedSetFetchByScore(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetFetchResponse.Miss.class);

    assertThat(client.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    // Full set ascending, end index larger than set
    assertThat(
            client.sortedSetFetchByScore(cacheName, sortedSetName, 0.0, 9.9, SortOrder.ASCENDING))
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
            client.sortedSetFetchByScore(
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
    assertThat(client.sortedSetFetchByScore(cacheName, sortedSetName, null, null, null, 1, 3))
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
    assertThat(client.sortedSetFetchByScore(cacheName, sortedSetName))
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
    assertThat(
            client.sortedSetFetchByScore(
                null, sortedSetName, 10.0, 0.5, SortOrder.ASCENDING, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithNullCacheName() {
    assertThat(client.sortedSetFetchByScore(null, sortedSetName, null, null, null, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithNonexistentCacheName() {
    assertThat(
            client.sortedSetFetchByScore(
                randomString("cache"), sortedSetName, null, null, null, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetFetchByScore(cacheName, null, null, null, null, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetGetRank

  @Test
  public void sortedSetGetRankStringHappyPath() {
    final String one = "1";
    final String two = "2";

    assertThat(client.sortedSetGetRank(cacheName, sortedSetName, one, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetGetRankResponse.Miss.class);

    assertThat(client.sortedSetPutElement(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetGetRank(cacheName, sortedSetName, one, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.rank()).isEqualTo(0));

    // Add another element that changes the rank of the first one
    assertThat(client.sortedSetPutElement(cacheName, sortedSetName, two, 0.5))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetGetRank(cacheName, sortedSetName, one, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.rank()).isEqualTo(1));

    // Check the descending rank
    assertThat(client.sortedSetGetRank(cacheName, sortedSetName, one, SortOrder.DESCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.rank()).isEqualTo(0));
  }

  @Test
  public void sortedSetGetRankReturnsErrorWithNullCacheName() {
    assertThat(client.sortedSetGetRank(null, sortedSetName, "element", SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.sortedSetGetRank(null, sortedSetName, "element".getBytes(), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetRankReturnsErrorWithNonexistentCacheName() {
    assertThat(
            client.sortedSetGetRank(
                randomString("cache"), sortedSetName, "element", SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(
            client.sortedSetGetRank(
                randomString("cache"), sortedSetName, "element".getBytes(), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetGetRankReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetGetRank(cacheName, null, "element", SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetGetRank(cacheName, null, "element".getBytes(), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetRankReturnsErrorWithNullElement() {
    assertThat(
            client.sortedSetGetRank(cacheName, sortedSetName, (String) null, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.sortedSetGetRank(cacheName, sortedSetName, (byte[]) null, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetRankResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetGetScore

  @Test
  public void sortedSetGetScoreStringHappyPath() {
    final String one = "1";
    final String two = "2";

    assertThat(client.sortedSetGetScore(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetGetScoreResponse.Miss.class);

    assertThat(client.sortedSetPutElement(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetGetScore(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.score()).isEqualTo(1.0));

    // Add another element that changes the rank of the first one
    assertThat(client.sortedSetPutElement(cacheName, sortedSetName, two, 0.5))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetGetScore(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.score()).isEqualTo(1.0));
  }

  @Test
  public void sortedSetGetScoreBytesHappyPath() {
    final byte[] one = "1".getBytes();
    final byte[] two = "2".getBytes();

    assertThat(client.sortedSetGetScore(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetGetScoreResponse.Miss.class);

    assertThat(client.sortedSetPutElement(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetGetScore(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.score()).isEqualTo(1.0));

    // Add another element that changes the rank of the first one
    assertThat(client.sortedSetPutElement(cacheName, sortedSetName, two, 0.5))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetGetScore(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Hit.class))
        .satisfies(hit -> assertThat(hit.score()).isEqualTo(1.0));
  }

  @Test
  public void sortedSetGetScoreReturnsErrorWithNullCacheName() {
    assertThat(client.sortedSetGetScore(null, sortedSetName, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetGetScore(null, sortedSetName, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetScoreReturnsErrorWithNonexistentCacheName() {
    assertThat(client.sortedSetGetScore(randomString("cache"), sortedSetName, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(client.sortedSetGetScore(randomString("cache"), sortedSetName, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetGetScoreReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetGetScore(cacheName, null, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetGetScore(cacheName, null, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetScoreReturnsErrorWithNullElement() {
    assertThat(client.sortedSetGetScore(cacheName, sortedSetName, (String) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetGetScore(cacheName, sortedSetName, (byte[]) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetGetScores

  @Test
  public void sortedSetGetScoresStringHappyPath() {
    final String one = "1";
    final String two = "2";
    final Set<String> elements = new HashSet<>();
    elements.add(one);
    elements.add(two);

    assertThat(client.sortedSetGetScores(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetGetScoresResponse.Miss.class);

    assertThat(client.sortedSetPutElement(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    // One element in the set, one not in the set
    assertThat(client.sortedSetGetScores(cacheName, sortedSetName, elements))
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
    assertThat(client.sortedSetPutElement(cacheName, sortedSetName, two, 0.5))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetGetScores(cacheName, sortedSetName, elements))
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
    assertThat(client.sortedSetGetScores(null, sortedSetName, Collections.singleton("element")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.sortedSetGetScoresByteArray(
                null, sortedSetName, Collections.singleton("element".getBytes())))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetScoresReturnsErrorWithNonexistentCacheName() {
    assertThat(
            client.sortedSetGetScores(
                randomString("cache"), sortedSetName, Collections.singleton("element")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(
            client.sortedSetGetScoresByteArray(
                randomString("cache"), sortedSetName, Collections.singleton("element".getBytes())))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetGetScoresReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetGetScores(cacheName, null, Collections.singleton("element")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.sortedSetGetScoresByteArray(
                cacheName, null, Collections.singleton("element".getBytes())))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetGetScoresReturnsErrorWithNullElements() {
    assertThat(client.sortedSetGetScores(cacheName, sortedSetName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetGetScoresByteArray(cacheName, sortedSetName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetGetScoresResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetIncrementScore

  @Test
  public void sortedSetIncrementScoreStringHappyPath() {
    final String one = "1";

    assertThat(client.sortedSetIncrementScore(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Success.class))
        .satisfies(success -> assertThat(success.score()).isEqualTo(1.0));

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.elementsList())
                    .hasSize(1)
                    .map(ScoredElement::getScore)
                    .containsOnly(1.0));

    assertThat(client.sortedSetIncrementScore(cacheName, sortedSetName, one, 14.5))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Success.class))
        .satisfies(success -> assertThat(success.score()).isEqualTo(15.5));

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.elementsList())
                    .hasSize(1)
                    .map(ScoredElement::getScore)
                    .containsOnly(15.5));

    assertThat(client.sortedSetIncrementScore(cacheName, sortedSetName, one, -115.5))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Success.class))
        .satisfies(success -> assertThat(success.score()).isEqualTo(-100));
  }

  @Test
  public void sortedSetIncrementScoreBytesHappyPath() {
    final byte[] one = "1".getBytes();

    assertThat(client.sortedSetIncrementScore(cacheName, sortedSetName, one, 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Success.class))
        .satisfies(success -> assertThat(success.score()).isEqualTo(1.0));

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.elementsList())
                    .hasSize(1)
                    .map(ScoredElement::getScore)
                    .containsOnly(1.0));

    assertThat(client.sortedSetIncrementScore(cacheName, sortedSetName, one, 14.5))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Success.class))
        .satisfies(success -> assertThat(success.score()).isEqualTo(15.5));

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetFetchResponse.Hit.class))
        .satisfies(
            hit ->
                assertThat(hit.elementsList())
                    .hasSize(1)
                    .map(ScoredElement::getScore)
                    .containsOnly(15.5));

    assertThat(client.sortedSetIncrementScore(cacheName, sortedSetName, one, -115.5))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Success.class))
        .satisfies(success -> assertThat(success.score()).isEqualTo(-100));
  }

  @Test
  public void sortedSetIncrementScoreReturnsErrorWithNullCacheName() {
    assertThat(client.sortedSetIncrementScore(null, sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetIncrementScore(null, sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetIncrementScoreReturnsErrorWithNonexistentCacheName() {
    assertThat(client.sortedSetIncrementScore(randomString("cache"), sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(
            client.sortedSetIncrementScore(
                randomString("cache"), sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetIncrementScoreReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetIncrementScore(cacheName, null, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetIncrementScore(cacheName, null, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetIncrementScoreReturnsErrorWithNullElement() {
    assertThat(
            client.sortedSetIncrementScore(
                cacheName, sortedSetName, (String) null, 1.0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.sortedSetIncrementScore(
                cacheName, sortedSetName, (byte[]) null, 1.0, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetIncrementScoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetRemoveElement

  @Test
  public void sortedSetRemoveElementStringHappyPath() {
    final String one = "1";
    final String two = "2";
    final String three = "3";
    final Map<String, Double> elements = ImmutableMap.of(one, 1.0, two, 2.0, three, 3.0);

    assertThat(client.sortedSetRemoveElement(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementResponse.Success.class);

    assertThat(client.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(client.sortedSetRemoveElement(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
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
    final byte[] one = "1".getBytes();
    final byte[] two = "2".getBytes();
    final byte[] three = "3".getBytes();
    final Map<byte[], Double> elements = ImmutableMap.of(one, 1.0, two, 2.0, three, 3.0);

    assertThat(client.sortedSetRemoveElement(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementResponse.Success.class);

    assertThat(client.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(client.sortedSetRemoveElement(cacheName, sortedSetName, one))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
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
    assertThat(client.sortedSetRemoveElement(null, sortedSetName, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetRemoveElement(null, sortedSetName, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetRemoveElementReturnsErrorWithNonexistentCacheName() {
    assertThat(client.sortedSetRemoveElement(randomString("cache"), sortedSetName, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(
            client.sortedSetRemoveElement(
                randomString("cache"), sortedSetName, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetRemoveElementReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetRemoveElement(cacheName, null, "element"))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetRemoveElement(cacheName, null, "element".getBytes()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetRemoveElementReturnsErrorWithNullElement() {
    assertThat(client.sortedSetRemoveElement(cacheName, sortedSetName, (String) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetRemoveElement(cacheName, sortedSetName, (byte[]) null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // sortedSetRemoveElements

  @Test
  public void sortedSetRemoveElementsStringHappyPath() {
    final String one = "1";
    final String two = "2";
    final String three = "3";
    final Map<String, Double> elements = ImmutableMap.of(one, 1.0, two, 2.0, three, 3.0);

    assertThat(client.sortedSetRemoveElements(cacheName, sortedSetName, elements.keySet()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementsResponse.Success.class);

    assertThat(client.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(client.sortedSetRemoveElements(cacheName, sortedSetName, Sets.newHashSet(one, two)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementsResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
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
    final byte[] one = "1".getBytes();
    final byte[] two = "2".getBytes();
    final byte[] three = "3".getBytes();
    final Map<byte[], Double> elements = ImmutableMap.of(one, 1.0, two, 2.0, three, 3.0);

    assertThat(client.sortedSetRemoveElementsByteArray(cacheName, sortedSetName, elements.keySet()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementsResponse.Success.class);

    assertThat(client.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetPutElementsResponse.Success.class);

    assertThat(
            client.sortedSetRemoveElementsByteArray(
                cacheName, sortedSetName, Sets.newHashSet(one, two)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SortedSetRemoveElementsResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
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
    assertThat(client.sortedSetRemoveElements(null, sortedSetName, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetRemoveElementsByteArray(null, sortedSetName, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetRemoveElementsReturnsErrorWithNonexistentCacheName() {
    assertThat(
            client.sortedSetRemoveElements(
                randomString("cache"), sortedSetName, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(
            client.sortedSetRemoveElementsByteArray(
                randomString("cache"), sortedSetName, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetRemoveElementsReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetRemoveElements(cacheName, null, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetRemoveElementsByteArray(cacheName, null, Collections.emptySet()))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetRemoveElementsReturnsErrorWithNullElements() {
    assertThat(client.sortedSetRemoveElements(cacheName, sortedSetName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetRemoveElementsByteArray(cacheName, sortedSetName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SortedSetRemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }
}
