package momento.client.example;

import static momento.client.example.ExampleUtils.logEndBanner;
import static momento.client.example.ExampleUtils.logStartBanner;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.sortedset.SortedSetFetchResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetScoresResponse;
import momento.sdk.responses.cache.sortedset.SortedSetIncrementScoreResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementsResponse;
import momento.sdk.responses.cache.sortedset.SortedSetRemoveElementResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortedSetExample {

  private static final String AUTH_TOKEN_ENV_VAR = "MOMENTO_AUTH_TOKEN";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  private static final String CACHE_NAME = "set-example-cache";
  private static final String SORTED_SET_NAME = "example-sorted-set";

  private static final Logger logger = LoggerFactory.getLogger(SortedSetExample.class);

  public static void main(String[] args) {
    logStartBanner(logger);

    final CredentialProvider credentialProvider;
    try {
      credentialProvider = new EnvVarCredentialProvider(AUTH_TOKEN_ENV_VAR);
    } catch (SdkException e) {
      logger.error("Unable to load credential from environment variable " + AUTH_TOKEN_ENV_VAR, e);
      throw e;
    }

    try (final CacheClient client =
        CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_ITEM_TTL)
            .build()) {
      // Create a cache
      final CacheCreateResponse createResponse = client.createCache(CACHE_NAME).join();
      if (createResponse instanceof CacheCreateResponse.Error error) {
        if (error.getCause() instanceof AlreadyExistsException) {
          logger.info("Cache with name '{}' already exists.", CACHE_NAME);
        } else {
          logger.error("Cache creation failed with error " + error.getErrorCode(), error);
        }
      }

      // add elements to a sorted set
      logger.info("Adding elements to " + SORTED_SET_NAME);

      final SortedSetPutElementResponse putElementResponse =
          client.sortedSetPutElement(CACHE_NAME, SORTED_SET_NAME, "field1", 1.0).join();
      if (putElementResponse instanceof SortedSetPutElementResponse.Error error) {
        logger.error("Sorted set put element failed with error " + error.getErrorCode(), error);
      }

      final Map<String, Double> elements = Map.of("field2", 2.0, "field3", 3.0, "field4", 4.0);
      final SortedSetPutElementsResponse putElementsResponse =
          client.sortedSetPutElements(CACHE_NAME, SORTED_SET_NAME, elements).join();
      if (putElementsResponse instanceof SortedSetPutElementsResponse.Error error) {
        logger.error("Sorted set add elements failed with error " + error.getErrorCode(), error);
      }

      // Fetch the sorted set
      logger.info("Fetching " + SORTED_SET_NAME);

      final SortedSetFetchResponse fetchResponse =
          client.sortedSetFetchByRank(CACHE_NAME, SORTED_SET_NAME).join();
      if (fetchResponse instanceof SortedSetFetchResponse.Hit hit) {
        logger.info(SORTED_SET_NAME + " has elements:");
        logger.info(
            hit.elementsList().stream()
                .map(e -> e.getValue() + ": " + e.getScore())
                .collect(Collectors.joining(", ")));
      } else if (fetchResponse instanceof SortedSetFetchResponse.Miss) {
        logger.info("Did not find sorted set with name " + SORTED_SET_NAME);
      } else if (fetchResponse instanceof SortedSetFetchResponse.Error error) {
        logger.error("Sorted set fetch failed with error " + error.getErrorCode(), error);
      }

      // Increment a score
      logger.info("Incrementing the score of an element");
      final SortedSetIncrementScoreResponse incrementResponse =
          client.sortedSetIncrementScore(CACHE_NAME, SORTED_SET_NAME, "field1", 999.0).join();
      if (incrementResponse instanceof SortedSetIncrementScoreResponse.Success success) {
        logger.info("field1 new score: " + success.score());
      } else if (fetchResponse instanceof SortedSetIncrementScoreResponse.Error error) {
        logger.error("Sorted set increment failed with error " + error.getErrorCode(), error);
      }

      // Remove an element
      logger.info("Removing an value from " + SORTED_SET_NAME);

      final SortedSetRemoveElementResponse removeResponse =
          client.sortedSetRemoveElement(CACHE_NAME, SORTED_SET_NAME, "field2").join();
      if (removeResponse instanceof SortedSetRemoveElementResponse.Error error) {
        logger.error("Sorted set remove failed with error " + error.getErrorCode(), error);
      }

      // Check the current scores
      logger.info("Checking the scores of all the elements");

      final Set<String> fields = Set.of("field1", "field2", "field3", "field4");
      final SortedSetGetScoresResponse getScoresResponse =
          client.sortedSetGetScores(CACHE_NAME, SORTED_SET_NAME, fields).join();
      if (getScoresResponse instanceof SortedSetGetScoresResponse.Hit hit) {
        logger.info(SORTED_SET_NAME + " has elements:");
        logger.info(
            hit.scoredElements().stream()
                .map(e -> e.getValue() + ": " + e.getScore())
                .collect(Collectors.joining(", ")));
      } else if (getScoresResponse instanceof SortedSetFetchResponse.Miss) {
        logger.info("Did not find sorted set with name " + SORTED_SET_NAME);
      } else if (getScoresResponse instanceof SortedSetGetScoresResponse.Error error) {
        logger.error("Sorted set get scores failed with error " + error.getErrorCode(), error);
      }
    }

    logEndBanner(logger);
  }
}
