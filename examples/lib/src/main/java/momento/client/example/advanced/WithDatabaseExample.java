package momento.client.example.advanced;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import momento.client.example.AbstractExample;
import momento.sdk.CacheClient;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;

/**
 * Example application that shows how to handle Cache lookups and cache misses.
 *
 * <p>An in-memory hashmap based database stores items(item_id, item_name). The example looks to
 * fetch a given item from cache first. If it is hit, it returns the data. On a cache miss, attempts
 * to read data from Database and then add the data to cache. Any future lookups within the bounds
 * set by TTL will result in a cache hit.
 */
public class WithDatabaseExample extends AbstractExample {

  private static final String CACHE_NAME = "cache";
  private static final String ITEM_NOT_FOUND_MESSAGE = "not found in Cache or Database";
  private static final List<String> itemIds = Arrays.asList("1", "20");

  public static void main(String[] args) {
    printStartBanner("Database");
    final Database database = new DatabaseImpl();
    try (final CacheClient cacheClient = buildCacheClient()) {
      createCache(cacheClient, CACHE_NAME);
      runExample(cacheClient, database);
    }
    printEndBanner("Database");
  }

  private static void runExample(CacheClient cache, Database database) {
    for (final String itemId : itemIds) {
      System.out.printf("Initiating Lookup for item id '%s'.%n", itemId);
      final String result = lookup(itemId, cache, database).orElse(ITEM_NOT_FOUND_MESSAGE);
      System.out.printf("Item id: %s, Item: %s%n", itemId, result);

      // If the item was found in the Database or Cache the second look up should be a cache hit.
      if (!result.equals(ITEM_NOT_FOUND_MESSAGE)) {
        System.out.printf("%n%nLookup Item id '%s' again.%n", itemId);

        final String secondLookup =
            lookup(itemId, cache, database)
                .orElseThrow(() -> new IllegalStateException("Item should be present."));
        System.out.printf("Item id: %s, Item: %s%n", itemId, secondLookup);
      }

      System.out.printf("Done looking up item id '%s'.%n%n%n", itemId);
    }
  }

  // Handle cache lookups and fallback to database when item isn't found
  private static Optional<String> lookup(String itemId, CacheClient cache, Database database) {
    final CacheGetResponse response = cache.get(CACHE_NAME, itemId).join();
    writeCacheLog(response, itemId);
    if (response instanceof CacheGetResponse.Hit hit) {
      return Optional.of(hit.valueString());
    } else {
      return handleCacheMiss(itemId, cache, database);
    }
  }

  // Handle Cache Miss
  // Lookup the item in database and if found, add the item to cache.
  private static Optional<String> handleCacheMiss(
      String itemId, CacheClient cache, Database database) {
    final Optional<String> item = database.getItem(itemId);
    if (item.isPresent()) {
      final CacheSetResponse response = cache.set(CACHE_NAME, itemId, item.get()).join();
      if (response instanceof CacheSetResponse.Success) {
        System.out.printf(
            "Item with key '%s' and value '%s' stored in Cache%n", itemId, item.get());
      } else {
        System.out.printf(
            "Failed to write item with key '%s' and value '%s' to Cache", itemId, item.get());
      }
    }
    return item;
  }

  private static void writeCacheLog(CacheGetResponse response, String key) {
    System.out.printf(
        "Cache lookup up for item id '%s' resulted in: %s%n",
        key, response.getClass().getSimpleName());
  }

  private static void createCache(CacheClient cacheClient, String cacheName) {
    try {
      cacheClient.createCache(cacheName);
    } catch (AlreadyExistsException e) {
      System.out.printf("Cache with name '%s' already exists.%n", cacheName);
    }
  }
}

/** Simple in-memory database */
class DatabaseImpl implements Database {
  private final Map<String, String> data;

  DatabaseImpl() {
    data = buildDatabase();
  }

  @Override
  public Optional<String> getItem(String itemId) {
    if (data.containsKey(itemId)) {
      System.out.printf("Item with id '%s' found in database%n", itemId);
      return Optional.ofNullable(data.get(itemId));
    }
    System.out.printf("Item with id '%s' not found in database%n", itemId);
    return Optional.empty();
  }

  private static Map<String, String> buildDatabase() {
    final Map<String, String> data = new HashMap<>();
    data.put("1", "Bananas");
    data.put("2", "Apples");
    data.put("3", "Mangoes");
    data.put("4", "Watermelon");
    return data;
  }
}

interface Database {
  Optional<String> getItem(String itemId);
}
