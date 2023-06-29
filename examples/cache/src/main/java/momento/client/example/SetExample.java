package momento.client.example;

import java.time.Duration;
import java.util.Set;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.set.SetAddElementResponse;
import momento.sdk.responses.cache.set.SetAddElementsResponse;
import momento.sdk.responses.cache.set.SetFetchResponse;
import momento.sdk.responses.cache.set.SetRemoveElementResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetExample {

  private static final String AUTH_TOKEN_ENV_VAR = "MOMENTO_AUTH_TOKEN";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  private static final String CACHE_NAME = "set-example-cache";
  private static final String SET_NAME = "example-set";

  private static final Logger logger = LoggerFactory.getLogger(SetExample.class);

  public static void main(String[] args) {
    logStartBanner();

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

      // Add elements to a set
      logger.info("Adding elements to " + SET_NAME);

      final SetAddElementResponse addElementResponse =
          client.setAddElement(CACHE_NAME, SET_NAME, "element1").join();
      if (addElementResponse instanceof SetAddElementResponse.Error error) {
        logger.error("Set add element failed with error " + error.getErrorCode(), error);
      }

      final Set<String> elements = Set.of("element2", "element3", "element4");
      final SetAddElementsResponse addElementsResponse =
          client.setAddElements(CACHE_NAME, SET_NAME, elements).join();
      if (addElementsResponse instanceof SetAddElementsResponse.Error error) {
        logger.error("Set add elements failed with error " + error.getErrorCode(), error);
      }

      // Fetch the set
      logger.info("Fetching " + SET_NAME);

      final SetFetchResponse fetchResponse = client.setFetch(CACHE_NAME, SET_NAME).join();
      if (fetchResponse instanceof SetFetchResponse.Hit hit) {
        final Set<String> fetchedElements = hit.valueSet();
        logger.info(SET_NAME + " has elements:");
        logger.info(String.join(", ", fetchedElements));
      } else if (fetchResponse instanceof SetFetchResponse.Miss) {
        logger.info("Did not find set with name " + SET_NAME);
      } else if (fetchResponse instanceof SetFetchResponse.Error error) {
        logger.error("Set fetch failed with error " + error.getErrorCode(), error);
      }

      // Remove an element
      logger.info("Removing an element from " + SET_NAME);

      final SetRemoveElementResponse removeResponse =
          client.setRemoveElement(CACHE_NAME, SET_NAME, "element2").join();
      if (removeResponse instanceof SetRemoveElementResponse.Error error) {
        logger.error("Set remove failed with error " + error.getErrorCode(), error);
      }

      // Fetch the now smaller set
      logger.info("Fetching {} once again", SET_NAME);

      final SetFetchResponse secondFetchResponse = client.setFetch(CACHE_NAME, SET_NAME).join();
      if (secondFetchResponse instanceof SetFetchResponse.Hit hit) {
        final Set<String> fetchedElements = hit.valueSet();
        logger.info(SET_NAME + " has elements:");
        logger.info(String.join(", ", fetchedElements));
      } else if (secondFetchResponse instanceof SetFetchResponse.Miss) {
        logger.info("Did not find set with name " + SET_NAME);
      } else if (secondFetchResponse instanceof SetFetchResponse.Error error) {
        logger.error("Set fetch failed with error " + error.getErrorCode(), error);
      }
    }

    logEndBanner();
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
