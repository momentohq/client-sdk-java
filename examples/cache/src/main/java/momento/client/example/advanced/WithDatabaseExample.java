package momento.client.example.advanced;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example application that shows how to handle Cache lookups and cache misses.
 *
 * <p>An in-memory hashmap based database stores items(item_id, item_name). The example looks to
 * fetch a given item from cache first. If it is hit, it returns the data. On a cache miss, attempts
 * to read data from Database and then add the data to cache. Any future lookups within the bounds
 * set by TTL will result in a cache hit.
 */
public class WithDatabaseExample {

  protected static final String AUTH_TOKEN_ENV_VAR = "MOMENTO_AUTH_TOKEN";
  protected static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  private static final String CACHE_NAME = "cache";
  private static final String ITEM_NOT_FOUND_MESSAGE = "not found in Cache or Database";
  private static final List<String> itemIds = Arrays.asList("1", "20");

  private static final Logger logger = LoggerFactory.getLogger(WithDatabaseExample.class);

  public static void main(String[] args) {
    logStartBanner();

    final Database database = new DatabaseImpl();
    try (final CacheClient cacheClient = createCacheClient()) {
      createCache(cacheClient, CACHE_NAME);
      runExample(cacheClient, database);
    }
    logEndBanner();
  }

  private static void runExample(CacheClient cache, Database database) {
    for (final String itemId : itemIds) {
      logger.info("Initiating Lookup for item id '{}'.", itemId);
      final String result = lookup(itemId, cache, database).orElse(ITEM_NOT_FOUND_MESSAGE);
      logger.info("Item id: {}, Item: {}", itemId, result);

      // If the item was found in the Database or Cache the second look up should be a cache hit.
      if (!result.equals(ITEM_NOT_FOUND_MESSAGE)) {
        logger.info("Lookup item id '{}' again.", itemId);

        final String secondLookup =
            lookup(itemId, cache, database)
                .orElseThrow(() -> new IllegalStateException("Item should be present."));
        logger.info("Item id: {}, Item: {}", itemId, secondLookup);
      }

      logger.info("Done looking up item id '{}'.", itemId);
    }
  }

  // Handle cache lookups and fallback to database when item isn't found
  private static Optional<String> lookup(String itemId, CacheClient cache, Database database) {
    final GetResponse response = cache.get(CACHE_NAME, itemId).join();
    writeCacheLog(response, itemId);
    if (response instanceof GetResponse.Hit hit) {
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
      final SetResponse response = cache.set(CACHE_NAME, itemId, item.get()).join();
      if (response instanceof SetResponse.Success) {
        logger.info("Item with key '{}' and value '{}' stored in Cache", itemId, item.get());
      } else if (response instanceof SetResponse.Error error) {
        logger
            .atError()
            .setCause(error)
            .log("Failed to write item with key '{}' and value '{}' to Cache", itemId, item.get());
      }
    }
    return item;
  }

  private static void writeCacheLog(GetResponse response, String key) {
    logger.info(
        "Cache lookup up for item id '{}' resulted in: {}",
        key,
        response.getClass().getSimpleName());
  }

  private static void createCache(CacheClient cacheClient, String cacheName) {
    final CacheCreateResponse createCacheResponse = cacheClient.createCache(cacheName).join();
    if (createCacheResponse instanceof CacheCreateResponse.Error error) {
      if (error.getCause() instanceof AlreadyExistsException) {
        logger.info("Cache with name '{}' already exists.", cacheName);
      } else {
        logger
            .atError()
            .setCause(error)
            .log("Error {} when creating cache with name {}", error.getErrorCode(), cacheName);
      }
    }
  }

  private static CacheClient createCacheClient() {
    final CredentialProvider credentialProvider;
    try {
      credentialProvider = new EnvVarCredentialProvider(AUTH_TOKEN_ENV_VAR);
    } catch (SdkException e) {
      logger.error("Unable to load credential from environment variable " + AUTH_TOKEN_ENV_VAR, e);
      throw e;
    }
    return CacheClient.create(credentialProvider, Configurations.Laptop.latest(), DEFAULT_ITEM_TTL, null);
  }

  private static void logStartBanner() {
    logger.info("******************************************************************");
    logger.info("Example Start");
    logger.info("******************************************************************");
  }

  private static void logEndBanner() {
    logger.info("******************************************************************");
    logger.info("Example End");
    logger.info("******************************************************************");
  }
}

/** Simple in-memory database */
class DatabaseImpl implements Database {
  private final Map<String, String> data;

  private static final Logger logger = LoggerFactory.getLogger(DatabaseImpl.class);

  DatabaseImpl() {
    data = buildDatabase();
  }

  @Override
  public Optional<String> getItem(String itemId) {
    if (data.containsKey(itemId)) {
      logger.info("Item with id '{}' found in database", itemId);
      return Optional.ofNullable(data.get(itemId));
    }
    logger.info("Item with id '{}' not found in database", itemId);
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
