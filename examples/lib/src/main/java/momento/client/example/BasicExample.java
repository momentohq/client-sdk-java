package momento.client.example;

import momento.sdk.CacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.ListCachesResponse;

public class BasicExample extends AbstractExample {

  private static final String CACHE_NAME = "cache";
  private static final String KEY = "key";
  private static final String VALUE = "value";

  public static void main(String[] args) {
    printStartBanner("Basic");
    try (final CacheClient cacheClient = buildCacheClient()) {

      createCache(cacheClient, CACHE_NAME);

      listCaches(cacheClient);

      System.out.printf("Setting key '%s', value '%s'%n", KEY, VALUE);
      cacheClient.set(CACHE_NAME, KEY, VALUE).join();

      System.out.printf("Getting value for key '%s'%n", KEY);

      final CacheGetResponse getResponse = cacheClient.get(CACHE_NAME, KEY).join();
      if (getResponse instanceof CacheGetResponse.Hit hit) {
        System.out.printf("Found value for key '%s': '%s'%n", KEY, hit.valueString());
      } else if (getResponse instanceof CacheGetResponse.Miss) {
        System.out.println("Found no value for key " + KEY);
      } else if (getResponse instanceof CacheGetResponse.Error error) {
        System.out.printf("Error occurred when looking up value for key '%s':%n", KEY);
        System.out.println(error.getMessage());
      }
    }
    printEndBanner("Basic");
  }

  private static void createCache(CacheClient cacheClient, String cacheName) {
    final CreateCacheResponse createCacheResponse = cacheClient.createCache(cacheName);
    if (createCacheResponse instanceof CreateCacheResponse.Error error) {
      if (error.getCause() instanceof AlreadyExistsException) {
        System.out.println("Cache with name '" + cacheName + "' already exists.");
      } else {
        System.out.println("Unable to create cache with error: " + error.getMessage());
      }
    }
  }

  private static void listCaches(CacheClient cacheClient) {
    System.out.println("Listing caches:");
    final ListCachesResponse listCachesResponse = cacheClient.listCaches();
    if (listCachesResponse instanceof ListCachesResponse.Success success) {
      for (CacheInfo cacheInfo : success.getCaches()) {
        System.out.println(cacheInfo.name());
      }
    } else if (listCachesResponse instanceof ListCachesResponse.Error error) {
      System.out.println("Error occurred listing caches:");
      System.out.println(error.getMessage());
    }
  }
}
