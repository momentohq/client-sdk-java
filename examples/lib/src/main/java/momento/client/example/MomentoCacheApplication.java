package momento.client.example;

import java.time.Duration;
import momento.sdk.CacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.ListCachesResponse;

public class MomentoCacheApplication {

  private static final String MOMENTO_AUTH_TOKEN = System.getenv("MOMENTO_AUTH_TOKEN");
  private static final String CACHE_NAME = "cache";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  public static void main(String[] args) {
    printStartBanner();
    try (final CacheClient cacheClient =
        CacheClient.builder(MOMENTO_AUTH_TOKEN, DEFAULT_ITEM_TTL).build()) {

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
    printEndBanner();
  }

  private static void createCache(CacheClient cacheClient, String cacheName) {
    try {
      cacheClient.createCache(cacheName);
    } catch (AlreadyExistsException e) {
      System.out.printf("Cache with name '%s' already exists.%n", cacheName);
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

  private static void printStartBanner() {
    System.out.println("******************************************************************");
    System.out.println("*                      Momento Example Start                     *");
    System.out.println("******************************************************************");
  }

  private static void printEndBanner() {
    System.out.println("******************************************************************");
    System.out.println("*                       Momento Example End                      *");
    System.out.println("******************************************************************");
  }
}
