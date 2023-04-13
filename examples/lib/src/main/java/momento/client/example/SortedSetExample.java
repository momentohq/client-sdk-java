package momento.client.example;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import momento.sdk.CacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheSetAddElementResponse;
import momento.sdk.messages.CacheSortedSetFetchResponse;
import momento.sdk.messages.CacheSortedSetGetScoresResponse;
import momento.sdk.messages.CacheSortedSetIncrementScoreResponse;
import momento.sdk.messages.CacheSortedSetPutElementResponse;
import momento.sdk.messages.CacheSortedSetPutElementsResponse;
import momento.sdk.messages.CacheSortedSetRemoveElementResponse;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.ScoredElement;

public class SortedSetExample extends AbstractExample {

  private static final String CACHE_NAME = "set-example-cache";
  private static final String SORTED_SET_NAME = "example-sorted-set";

  public static void main(String[] args) {
    printStartBanner("Sorted Set");

    try (final CacheClient client = buildCacheClient()) {
      // create a cache
      final CreateCacheResponse createCacheResponse = client.createCache(CACHE_NAME);
      if (createCacheResponse instanceof CreateCacheResponse.Error error) {
        if (error.getCause() instanceof AlreadyExistsException) {
          System.out.println("Cache with name '" + CACHE_NAME + "' already exists.");
        } else {
          System.out.println("Unable to create cache with error: " + error.getMessage());
        }
      }

      // add elements to a sorted set
      System.out.println("Adding elements to " + SORTED_SET_NAME);

      final CacheSortedSetPutElementResponse putElementResponse =
          client.sortedSetPutElement(CACHE_NAME, SORTED_SET_NAME, "field1", 1.0).join();
      if (putElementResponse instanceof CacheSetAddElementResponse.Error error) {
        System.out.println("Sorted set put element failed with error: " + error.getMessage());
      }

      final Map<String, Double> elements = Map.of("field2", 2.0, "field3", 3.0, "field4", 4.0);
      final CacheSortedSetPutElementsResponse putElementsResponse =
          client.sortedSetPutElements(CACHE_NAME, SORTED_SET_NAME, elements).join();
      if (putElementsResponse instanceof CacheSortedSetPutElementsResponse.Error error) {
        System.out.println("Sorted set add elements failed with error: " + error.getMessage());
      }

      // fetch the sorted set
      System.out.println("Fetching " + SORTED_SET_NAME);

      final CacheSortedSetFetchResponse fetchResponse =
          client.sortedSetFetchByRank(CACHE_NAME, SORTED_SET_NAME).join();
      if (fetchResponse instanceof CacheSortedSetFetchResponse.Hit hit) {
        final List<ScoredElement> fetchedElements = hit.elementsList();
        System.out.println(SORTED_SET_NAME + " has elements:");
        System.out.println(
            fetchedElements.stream()
                .map(e -> e.getElement() + ": " + e.getScore())
                .collect(Collectors.joining("\n")));
      } else if (fetchResponse instanceof CacheSortedSetFetchResponse.Miss) {
        System.out.println("Did not find sorted set with name " + SORTED_SET_NAME);
      } else if (fetchResponse instanceof CacheSortedSetFetchResponse.Error error) {
        System.out.println("Sorted set fetch failed with error: " + error.getMessage());
      }

      // Increment a score
      System.out.println("Incrementing the score of an element");
      final CacheSortedSetIncrementScoreResponse incrementResponse =
          client.sortedSetIncrementScore(CACHE_NAME, SORTED_SET_NAME, "field1", 999.0).join();
      if (incrementResponse instanceof CacheSortedSetIncrementScoreResponse.Success success) {
        System.out.println("field1 new score: " + success.score());
      }

      // Remove an element
      System.out.println("Removing an element from " + SORTED_SET_NAME);

      final CacheSortedSetRemoveElementResponse removeResponse =
          client.sortedSetRemoveElement(CACHE_NAME, SORTED_SET_NAME, "field2").join();
      if (removeResponse instanceof CacheSortedSetRemoveElementResponse.Error error) {
        System.out.println("Sorted set remove failed with error: " + error.getMessage());
      }

      // Check the current scores
      System.out.println("Checking the scores of all the elements");

      final Set<String> fields = Set.of("field1", "field2", "field3", "field4");
      final CacheSortedSetGetScoresResponse getScoresResponse =
          client.sortedSetGetScores(CACHE_NAME, SORTED_SET_NAME, fields).join();
      if (getScoresResponse instanceof CacheSortedSetGetScoresResponse.Hit hit) {
        System.out.println(SORTED_SET_NAME + " has elements:");
        final List<ScoredElement> fetchedElements = hit.scoredElements();
        System.out.println(
            fetchedElements.stream()
                .map(e -> e.getElement() + ": " + e.getScore())
                .collect(Collectors.joining("\n")));
      } else if (getScoresResponse instanceof CacheSortedSetFetchResponse.Miss) {
        System.out.println("Did not find sorted set with name " + SORTED_SET_NAME);
      } else if (getScoresResponse instanceof CacheSortedSetGetScoresResponse.Error error) {
        System.out.println("Sorted set get scores failed with error: " + error.getMessage());
      }
    }

    printEndBanner("Sorted Set");
  }
}
