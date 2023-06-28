package momento.client.example;

import static momento.client.example.ExampleUtils.logEndBanner;
import static momento.client.example.ExampleUtils.logStartBanner;

import java.time.Duration;
import java.util.List;
import momento.client.example.util.AuthUtil;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.list.ListConcatenateFrontResponse;
import momento.sdk.responses.cache.list.ListFetchResponse;
import momento.sdk.responses.cache.list.ListLengthResponse;
import momento.sdk.responses.cache.list.ListPushFrontResponse;
import momento.sdk.responses.cache.list.ListRemoveValueResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListExample {
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);
  private static final String CACHE_NAME = "list-example-cache";
  private static final String LIST_NAME = "example-list";
  private static final Logger logger = LoggerFactory.getLogger(ListExample.class);

  public static void main(String[] args) {
    logStartBanner(logger);

    final CredentialProvider credentialProvider = AuthUtil.getCredentials();

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

      // Add elements to a list
      logger.info("Adding elements to " + LIST_NAME);

      final ListPushFrontResponse pushResponse =
          client.listPushFront(CACHE_NAME, LIST_NAME, "element4", 100).join();
      if (pushResponse instanceof ListPushFrontResponse.Error error) {
        logger.error("List push failed with error: " + error.getMessage(), error);
      }

      final List<String> elements = List.of("element1", "element2", "element2", "element3");
      final ListConcatenateFrontResponse concatenateResponse =
          client.listConcatenateFront(CACHE_NAME, LIST_NAME, elements, 100).join();
      if (concatenateResponse instanceof ListConcatenateFrontResponse.Error error) {
        logger.error("List concatenate failed with error " + error.getErrorCode(), error);
      }

      // Fetch the list
      logger.info("Fetching " + LIST_NAME);

      final ListFetchResponse fetchResponse =
          client.listFetch(CACHE_NAME, LIST_NAME, null, null).join();
      if (fetchResponse instanceof ListFetchResponse.Hit hit) {
        final List<String> fetchedElements = hit.valueList();
        logger.info("{} has length {} with elements:", LIST_NAME, fetchedElements.size());
        logger.info(String.join(", ", fetchedElements));
      } else if (fetchResponse instanceof ListFetchResponse.Miss) {
        logger.info("Did not find list with name " + LIST_NAME);
      } else if (fetchResponse instanceof ListFetchResponse.Error error) {
        logger.error("List fetch failed with error " + error.getErrorCode(), error);
      }

      // Remove an element
      logger.info("Removing an element that occurs multiple times from " + LIST_NAME);

      final ListRemoveValueResponse removeResponse =
          client.listRemoveValue(CACHE_NAME, LIST_NAME, "element2").join();
      if (removeResponse instanceof ListRemoveValueResponse.Error error) {
        logger.error("List remove failed with error " + error.getErrorCode(), error);
      }

      // Check the new list length
      logger.info("Checking length of " + LIST_NAME);

      final ListLengthResponse lengthResponse = client.listLength(CACHE_NAME, LIST_NAME).join();
      if (lengthResponse instanceof ListLengthResponse.Hit hit) {
        logger.info("{} length: {}", LIST_NAME, hit.getListLength());
      } else if (lengthResponse instanceof ListLengthResponse.Miss) {
        logger.info("Did not find list with name " + LIST_NAME);
      } else if (lengthResponse instanceof ListLengthResponse.Error error) {
        logger.error("List length failed with error " + error.getErrorCode(), error);
      }
    }
    logEndBanner(logger);
  }
}
