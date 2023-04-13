package momento.client.example;

import java.util.Map;
import java.util.stream.Collectors;
import momento.sdk.CacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheDictionaryFetchResponse;
import momento.sdk.messages.CacheDictionaryRemoveFieldResponse;
import momento.sdk.messages.CacheDictionarySetFieldResponse;
import momento.sdk.messages.CacheDictionarySetFieldsResponse;
import momento.sdk.messages.CreateCacheResponse;

public class DictionaryExample extends AbstractExample {

  private static final String CACHE_NAME = "dictionary-example-cache";
  private static final String DICTIONARY_NAME = "example-dictionary";

  public static void main(String[] args) {
    printStartBanner("Dictionary");

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

      // add elements to a dictionary
      System.out.println("Adding elements to " + DICTIONARY_NAME);

      final CacheDictionarySetFieldResponse setFieldResponse =
          client.dictionarySetField(CACHE_NAME, DICTIONARY_NAME, "field1", "value1").join();
      if (setFieldResponse instanceof CacheDictionarySetFieldResponse.Error error) {
        System.out.println("Dictionary set field failed with error: " + error.getMessage());
      }

      final Map<String, String> elements = Map.of("field2", "value2", "field3", "value3");
      final CacheDictionarySetFieldsResponse setFieldsResponse =
          client.dictionarySetFields(CACHE_NAME, DICTIONARY_NAME, elements).join();
      if (setFieldsResponse instanceof CacheDictionarySetFieldsResponse.Error error) {
        System.out.println("Dictionary set fields failed with error: " + error.getMessage());
      }

      // fetch the dictionary
      System.out.println("Fetching " + DICTIONARY_NAME);

      final CacheDictionaryFetchResponse fetchResponse =
          client.dictionaryFetch(CACHE_NAME, DICTIONARY_NAME).join();
      if (fetchResponse instanceof CacheDictionaryFetchResponse.Hit hit) {
        final Map<String, String> fetchedElements = hit.valueDictionary();
        System.out.println(DICTIONARY_NAME + " has elements:");
        System.out.println(
            fetchedElements.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ")));
      } else if (fetchResponse instanceof CacheDictionaryFetchResponse.Miss) {
        System.out.println("Did not find dictionary with name " + DICTIONARY_NAME);
      } else if (fetchResponse instanceof CacheDictionaryFetchResponse.Error error) {
        System.out.println("Dictionary fetch failed with error: " + error.getMessage());
      }

      // Remove an element
      System.out.println("Removing an element from " + DICTIONARY_NAME);

      final CacheDictionaryRemoveFieldResponse removeResponse =
          client.dictionaryRemoveField(CACHE_NAME, DICTIONARY_NAME, "field2").join();
      if (removeResponse instanceof CacheDictionaryRemoveFieldResponse.Error error) {
        System.out.println("Dictionary remove failed with error: " + error.getMessage());
      }

      // Fetch the now smaller dictionary
      System.out.println("Fetching " + DICTIONARY_NAME + " once again");

      final CacheDictionaryFetchResponse secondFetchResponse =
          client.dictionaryFetch(CACHE_NAME, DICTIONARY_NAME).join();
      if (secondFetchResponse instanceof CacheDictionaryFetchResponse.Hit hit) {
        final Map<String, String> fetchedElements = hit.valueDictionary();
        System.out.println(DICTIONARY_NAME + " has elements:");
        System.out.println(
            fetchedElements.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ")));
      } else if (secondFetchResponse instanceof CacheDictionaryFetchResponse.Miss) {
        System.out.println("Did not find dictionary with name " + DICTIONARY_NAME);
      } else if (secondFetchResponse instanceof CacheDictionaryFetchResponse.Error error) {
        System.out.println("Dictionary fetch failed with error: " + error.getMessage());
      }

      printEndBanner("Dictionary");
    }
  }
}
