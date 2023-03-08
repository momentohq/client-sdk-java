package momento.client.example;

import java.util.Optional;
import momento.sdk.SimpleCacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.ListCachesResponse;

public class MomentoCacheApplication {

  private static final String MOMENTO_AUTH_TOKEN = System.getenv("MOMENTO_AUTH_TOKEN");
  private static final String CACHE_NAME = "cache";
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final int DEFAULT_ITEM_TTL_SECONDS = 60;

  public static void main(String[] args) {
    printStartBanner();
    try (SimpleCacheClient simpleCacheClient =
        SimpleCacheClient.builder(MOMENTO_AUTH_TOKEN, DEFAULT_ITEM_TTL_SECONDS).build()) {

      createCache(simpleCacheClient, CACHE_NAME);

      listCaches(simpleCacheClient);

      System.out.println(String.format("Setting key=`%s` , value=`%s`", KEY, VALUE));
      simpleCacheClient.set(CACHE_NAME, KEY, VALUE);

      System.out.println(String.format("Getting value for key=`%s`", KEY));

      { // Java 8 style
        final CacheGetResponse getResponse = simpleCacheClient.getAsync(CACHE_NAME, KEY).join();
        if (getResponse instanceof CacheGetResponse.Hit) {
          final CacheGetResponse.Hit hit = (CacheGetResponse.Hit) getResponse;
          System.out.println("Hit! Found value: " + hit.valueString());
        } else if (getResponse instanceof CacheGetResponse.Miss) {
          System.out.println("Miss!");
        } else if (getResponse instanceof CacheGetResponse.Error) {
          final CacheGetResponse.Error error = (CacheGetResponse.Error) getResponse;
          System.out.println("Error encountered: " + error.exception().toString());
        } else {
          System.out.println("Unknown response type.");
        }
      }

      { // Java 14 style
        final CacheGetResponse getResponse = simpleCacheClient.getAsync(CACHE_NAME, KEY).join();
        if (getResponse instanceof CacheGetResponse.Hit hit) {
          System.out.println("Hit! Found value: " + hit.valueString());
        } else if (getResponse instanceof CacheGetResponse.Miss) {
          System.out.println("Miss!");
        } else if (getResponse instanceof CacheGetResponse.Error error) {
          System.out.println("Error encountered: " + error.exception().toString());
        } else {
          System.out.println("Unknown response type.");
        }
      }
    }
    printEndBanner();
  }

  private static void createCache(SimpleCacheClient simpleCacheClient, String cacheName) {
    try {
      simpleCacheClient.createCache(cacheName);
    } catch (AlreadyExistsException e) {
      System.out.println(String.format("Cache with name `%s` already exists.", cacheName));
    }
  }

  private static void listCaches(SimpleCacheClient simpleCacheClient) {
    System.out.println("Listing caches:");
    Optional<String> token = Optional.empty();
    do {
      ListCachesResponse listCachesResponse = simpleCacheClient.listCaches(token);
      for (CacheInfo cacheInfo : listCachesResponse.caches()) {
        System.out.println(cacheInfo.name());
      }
      token = listCachesResponse.nextPageToken();
    } while (token.isPresent());
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
