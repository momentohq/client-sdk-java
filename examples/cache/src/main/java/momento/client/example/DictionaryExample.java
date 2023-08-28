package momento.client.example;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.dictionary.DictionaryFetchResponse;
import momento.sdk.responses.cache.dictionary.DictionaryRemoveFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionarySetFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionarySetFieldsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DictionaryExample {

  private static final String AUTH_TOKEN_ENV_VAR = "MOMENTO_AUTH_TOKEN";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  private static final String CACHE_NAME = "dictionary-example-cache";
  private static final String DICTIONARY_NAME = "example-dictionary";

  private static final Logger logger = LoggerFactory.getLogger(DictionaryExample.class);

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
        CacheClient.create(
            credentialProvider, Configurations.Laptop.latest(), DEFAULT_ITEM_TTL, null)) {

      // Create a cache
      final CacheCreateResponse createResponse = client.createCache(CACHE_NAME).join();
      if (createResponse instanceof CacheCreateResponse.Error error) {
        if (error.getCause() instanceof AlreadyExistsException) {
          logger.info("Cache with name '{}' already exists.", CACHE_NAME);
        } else {
          logger.error("Cache creation failed with error " + error.getErrorCode(), error);
        }
      }

      // Add elements to a dictionary
      logger.info("Adding elements to " + DICTIONARY_NAME);

      final DictionarySetFieldResponse setFieldResponse =
          client.dictionarySetField(CACHE_NAME, DICTIONARY_NAME, "field1", "value1").join();
      if (setFieldResponse instanceof DictionarySetFieldResponse.Error error) {
        logger.error("Dictionary set field failed with error " + error.getErrorCode(), error);
      }

      final Map<String, String> elements = Map.of("field2", "value2", "field3", "value3");
      final DictionarySetFieldsResponse setFieldsResponse =
          client.dictionarySetFields(CACHE_NAME, DICTIONARY_NAME, elements).join();
      if (setFieldsResponse instanceof DictionarySetFieldsResponse.Error error) {
        logger.error("Dictionary set fields failed with error " + error.getErrorCode(), error);
      }

      // Fetch the dictionary
      logger.info("Fetching " + DICTIONARY_NAME);

      final DictionaryFetchResponse fetchResponse =
          client.dictionaryFetch(CACHE_NAME, DICTIONARY_NAME).join();
      if (fetchResponse instanceof DictionaryFetchResponse.Hit hit) {
        final Map<String, String> fetchedElements = hit.valueMap();
        logger.info(DICTIONARY_NAME + " elements:");
        logger.info(
            fetchedElements.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ")));
      } else if (fetchResponse instanceof DictionaryFetchResponse.Miss) {
        logger.info("Did not find dictionary with name " + DICTIONARY_NAME);
      } else if (fetchResponse instanceof DictionaryFetchResponse.Error error) {
        logger.error("Dictionary fetch failed with error " + error.getErrorCode(), error);
      }

      // Remove an element
      logger.info("Removing an element from " + DICTIONARY_NAME);

      final DictionaryRemoveFieldResponse removeResponse =
          client.dictionaryRemoveField(CACHE_NAME, DICTIONARY_NAME, "field2").join();
      if (removeResponse instanceof DictionaryRemoveFieldResponse.Error error) {
        logger.error("Dictionary remove field failed with error " + error.getErrorCode(), error);
      }

      // Fetch the now smaller dictionary
      logger.info("Fetching {} once again", DICTIONARY_NAME);

      final DictionaryFetchResponse secondFetchResponse =
          client.dictionaryFetch(CACHE_NAME, DICTIONARY_NAME).join();
      if (secondFetchResponse instanceof DictionaryFetchResponse.Hit hit) {
        final Map<String, String> fetchedElements = hit.valueMap();
        logger.info(DICTIONARY_NAME + " elements:");
        logger.info(
            fetchedElements.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ")));
      } else if (secondFetchResponse instanceof DictionaryFetchResponse.Miss) {
        logger.info("Did not find dictionary with name " + DICTIONARY_NAME);
      } else if (secondFetchResponse instanceof DictionaryFetchResponse.Error error) {
        logger.error("Dictionary fetch failed with error " + error.getErrorCode(), error);
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
