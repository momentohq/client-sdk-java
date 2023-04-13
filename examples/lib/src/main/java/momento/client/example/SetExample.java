package momento.client.example;

import java.util.Set;
import momento.sdk.CacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheSetAddElementResponse;
import momento.sdk.messages.CacheSetAddElementsResponse;
import momento.sdk.messages.CacheSetFetchResponse;
import momento.sdk.messages.CacheSetRemoveElementResponse;
import momento.sdk.messages.CreateCacheResponse;

public class SetExample extends AbstractExample {

  private static final String CACHE_NAME = "set-example-cache";
  private static final String SET_NAME = "example-set";

  public static void main(String[] args) {
    printStartBanner("Set");

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

      // add elements to a set
      System.out.println("Adding elements to " + SET_NAME);

      final CacheSetAddElementResponse addElementResponse =
          client.setAddElement(CACHE_NAME, SET_NAME, "element1").join();
      if (addElementResponse instanceof CacheSetAddElementResponse.Error error) {
        System.out.println("Set add element failed with error: " + error.getMessage());
      }

      final Set<String> elements = Set.of("element2", "element3", "element4");
      final CacheSetAddElementsResponse addElementsResponse =
          client.setAddElements(CACHE_NAME, SET_NAME, elements).join();
      if (addElementsResponse instanceof CacheSetAddElementsResponse.Error error) {
        System.out.println("Set add elements failed with error: " + error.getMessage());
      }

      // fetch the set
      System.out.println("Fetching " + SET_NAME);

      final CacheSetFetchResponse fetchResponse = client.setFetch(CACHE_NAME, SET_NAME).join();
      if (fetchResponse instanceof CacheSetFetchResponse.Hit hit) {
        final Set<String> fetchedElements = hit.valueSet();
        System.out.println(SET_NAME + " has elements:");
        System.out.println(String.join(", ", fetchedElements));
      } else if (fetchResponse instanceof CacheSetFetchResponse.Miss) {
        System.out.println("Did not find set with name " + SET_NAME);
      } else if (fetchResponse instanceof CacheSetFetchResponse.Error error) {
        System.out.println("Set fetch failed with error: " + error.getMessage());
      }

      // Remove an element
      System.out.println("Removing an element from " + SET_NAME);

      final CacheSetRemoveElementResponse removeResponse =
          client.setRemoveElement(CACHE_NAME, SET_NAME, "element2").join();
      if (removeResponse instanceof CacheSetRemoveElementResponse.Error error) {
        System.out.println("Set remove failed with error: " + error.getMessage());
      }

      // Fetch the now smaller set
      System.out.println("Fetching " + SET_NAME + " once again");

      final CacheSetFetchResponse secondFetchResponse =
          client.setFetch(CACHE_NAME, SET_NAME).join();
      if (secondFetchResponse instanceof CacheSetFetchResponse.Hit hit) {
        final Set<String> fetchedElements = hit.valueSet();
        System.out.println(SET_NAME + " has elements:");
        System.out.println(String.join(", ", fetchedElements));
      } else if (secondFetchResponse instanceof CacheSetFetchResponse.Miss) {
        System.out.println("Did not find set with name " + SET_NAME);
      } else if (secondFetchResponse instanceof CacheSetFetchResponse.Error error) {
        System.out.println("Set fetch failed with error: " + error.getMessage());
      }
    }

    printEndBanner("Set");
  }
}
