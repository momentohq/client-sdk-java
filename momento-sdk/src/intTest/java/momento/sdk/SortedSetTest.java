package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.messages.CacheSortedSetFetchResponse;
import momento.sdk.messages.CacheSortedSetPutElementResponse;
import momento.sdk.messages.CacheSortedSetPutElementsResponse;
import momento.sdk.messages.ScoredElement;
import momento.sdk.messages.SortOrder;
import momento.sdk.requests.CollectionTtl;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SortedSetTest {
  private static final Duration DEFAULT_TTL = Duration.ofSeconds(60);
  private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

  private final CredentialProvider credentialProvider =
      new EnvVarCredentialProvider("TEST_AUTH_TOKEN");
  private final String cacheName = System.getenv("TEST_CACHE_NAME");
  private CacheClient client;

  private String sortedSetName;

  @BeforeEach
  void setup() {
    client =
        CacheClient.builder(credentialProvider, Configurations.Laptop.Latest(), DEFAULT_TTL)
            .build();
    client.createCache(cacheName);
    sortedSetName = randomString("sortedSet");
  }

  @AfterEach
  void teardown() {
    client.deleteCache(cacheName);
    client.close();
  }

  // sortedSetPutElement

  @Test
  public void sortedSetPutElementStringHappyPath() {
    final String element = "1";
    final double score = 1.0;

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetFetchResponse.Miss.class);

    assertThat(
            client.sortedSetPutElement(
                cacheName, sortedSetName, element, score, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).map(ScoredElement::getElement).containsOnly(element);
              assertThat(scoredElements).map(ScoredElement::getScore).containsOnly(score);
            });
  }

  @Test
  public void sortedSetPutElementBytesHappyPath() {
    final byte[] element = "1".getBytes();
    final double score = 1.0;

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetFetchResponse.Miss.class);

    assertThat(
            client.sortedSetPutElement(
                cacheName, sortedSetName, element, score, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetPutElementResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements)
                  .map(ScoredElement::getElementByteArray)
                  .containsOnly(element);
              assertThat(scoredElements).map(ScoredElement::getScore).containsOnly(score);
            });
  }

  @Test
  public void sortedSetPutElementReturnsErrorWithNullCacheName() {
    assertThat(client.sortedSetPutElement(null, sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetPutElement(null, sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetPutElementReturnsErrorWithNonexistentCacheName() {
    assertThat(client.sortedSetPutElement(randomString("cache"), sortedSetName, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(
            client.sortedSetPutElement(
                randomString("cache"), sortedSetName, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetPutElementReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetPutElement(cacheName, null, "element", 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.sortedSetPutElement(cacheName, null, "element".getBytes(), 1.0))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementResponse.Error.class))
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
        .isInstanceOf(CacheSortedSetFetchResponse.Miss.class);

    assertThat(
            client.sortedSetPutElements(
                cacheName, sortedSetName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetPutElementsResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements)
                  .map(ScoredElement::getElement)
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
        .isInstanceOf(CacheSortedSetFetchResponse.Miss.class);

    assertThat(
            client.sortedSetPutElementsByteArray(
                cacheName, sortedSetName, elements, CollectionTtl.fromCacheTtl()))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetPutElementsResponse.Success.class);

    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
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
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.sortedSetPutElementsByteArray(
                null, sortedSetName, Collections.singletonMap("element".getBytes(), 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetPutElementsReturnsErrorWithNonexistentCacheName() {
    assertThat(
            client.sortedSetPutElements(
                randomString("cache"), sortedSetName, Collections.singletonMap("element", 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));

    assertThat(
            client.sortedSetPutElementsByteArray(
                randomString("cache"),
                sortedSetName,
                Collections.singletonMap("element".getBytes(), 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetPutElementsReturnsErrorWithNullSetName() {
    assertThat(
            client.sortedSetPutElements(cacheName, null, Collections.singletonMap("element", 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(
            client.sortedSetPutElementsByteArray(
                cacheName, null, Collections.singletonMap("element".getBytes(), 1.0)))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetPutElementsResponse.Error.class))
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
        .isInstanceOf(CacheSortedSetFetchResponse.Miss.class);

    assertThat(client.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetPutElementsResponse.Success.class);

    // Full set ascending, end index larger than set
    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName, 0, 6, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(5);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getElement)
                  .containsSequence(one, three, two, five, four);
            });

    // Partial set descending
    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName, 1, 4, SortOrder.DESCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(3);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getElement)
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
        .isInstanceOf(CacheSortedSetFetchResponse.Miss.class);

    assertThat(client.sortedSetPutElementsByteArray(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetPutElementsResponse.Success.class);

    // Full set ascending, end index larger than set
    assertThat(client.sortedSetFetchByRank(cacheName, sortedSetName, 0, 6, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
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
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
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
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithNullCacheName() {
    assertThat(client.sortedSetFetchByRank(null, sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithNonexistentCacheName() {
    assertThat(client.sortedSetFetchByRank(randomString("cache"), sortedSetName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetFetchByRankReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetFetchByRank(cacheName, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Error.class))
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

    assertThat(client.sortedSetFetchByScore(cacheName, sortedSetName, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetFetchResponse.Miss.class);

    assertThat(client.sortedSetPutElements(cacheName, sortedSetName, elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheSortedSetPutElementsResponse.Success.class);

    // Full set ascending, end index larger than set
    assertThat(
            client.sortedSetFetchByScore(
                cacheName, sortedSetName, 0.0, 9.9, SortOrder.ASCENDING, null, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(5);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getElement)
                  .containsSequence(one, three, two, five, four);
            });

    // Partial set descending
    assertThat(
            client.sortedSetFetchByScore(
                cacheName, sortedSetName, 0.2, 1.9, SortOrder.DESCENDING, 0, 99))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(3);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getElement)
                  .containsSequence(five, two, three);
            });

    // Partial set limited by offset and count
    assertThat(client.sortedSetFetchByScore(cacheName, sortedSetName, 1, 3))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Hit.class))
        .satisfies(
            hit -> {
              final List<ScoredElement> scoredElements = hit.elementsList();
              assertThat(scoredElements).hasSize(3);
              // check ordering
              assertThat(scoredElements)
                  .map(ScoredElement::getElement)
                  .containsSequence(three, two, five);
            });
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithInvalidScoreRange() {
    assertThat(
            client.sortedSetFetchByScore(
                null, sortedSetName, 10.0, 0.5, SortOrder.ASCENDING, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithNullCacheName() {
    assertThat(client.sortedSetFetchByScore(null, sortedSetName, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithNonexistentCacheName() {
    assertThat(client.sortedSetFetchByScore(randomString("cache"), sortedSetName, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void sortedSetFetchByScoreReturnsErrorWithNullSetName() {
    assertThat(client.sortedSetFetchByScore(cacheName, null, 0, 100))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheSortedSetFetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }
}
