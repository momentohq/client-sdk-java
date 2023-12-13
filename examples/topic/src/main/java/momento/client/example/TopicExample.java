package momento.client.example;

import java.time.Duration;

import io.grpc.Status;
import momento.sdk.CacheClient;
import momento.sdk.ISubscriptionCallbacks;
import momento.sdk.TopicClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.config.TopicConfigurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicExample {

    private static final String API_KEY_ENV_VAR = "MOMENTO_API_KEY";
    private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

    private static final String CACHE_NAME = "topic-example-cache";
    private static final String TOPIC_NAME = "example-topic";

    private static final Logger logger = LoggerFactory.getLogger(TopicExample.class);

    public static void main(String[] args) {
        logStartBanner();

        final CredentialProvider credentialProvider;
        try {
            credentialProvider = new EnvVarCredentialProvider(API_KEY_ENV_VAR);
        } catch (SdkException e) {
            logger.error("Unable to load credential from environment variable " + API_KEY_ENV_VAR, e);
            throw e;
        }

        try (final CacheClient client =
                     CacheClient.create(
                             credentialProvider, Configurations.Laptop.latest(), DEFAULT_ITEM_TTL);
             final TopicClient topicClient =
                     TopicClient.create(credentialProvider, TopicConfigurations.Laptop.latest()); ) {

            // Create a cache
            final CacheCreateResponse createResponse = client.createCache(CACHE_NAME).join();
            if (createResponse instanceof CacheCreateResponse.Error error) {
                if (error.getCause() instanceof AlreadyExistsException) {
                    logger.info("Cache with name '{}' already exists.", CACHE_NAME);
                } else {
                    logger.error("Cache creation failed with error " + error.getErrorCode(), error);
                }
            }

            // Subscribe to a topic
            final TopicSubscribeResponse subscribeResponse =
                    topicClient
                            .subscribe(
                                    CACHE_NAME,
                                    TOPIC_NAME,
                                    new ISubscriptionCallbacks() {
                                        @Override
                                        public void onItem(TopicMessage message) {
                                            logger.info("Received message: " + message.toString());
                                        }

                                        @Override
                                        public void onError(Throwable error) {
                                            logger.error("Subscription failed with error", error);
                                        }

                                        @Override
                                        public void onCompleted() {
                                            logger.info("Subscription completed");
                                        }
                                    })
                            .join();
            TopicSubscribeResponse.Subscription subscription = subscribeResponse.orElseThrow(
                    () -> new RuntimeException("Unable to subscribe to topic"));

            // Publish message to a topic
            for (int i = 0; i <= 100; i++) {
                System.out.println("Attempting to publish message #" + i);
                final TopicPublishResponse publishResponse =
                        topicClient.publish(CACHE_NAME, TOPIC_NAME, "message " + i).join();
                if (publishResponse instanceof TopicPublishResponse.Error error) {
                    logger.error("Topic publish failed with error " + error.getErrorCode(), error);
                }
                Thread.sleep(1000);
            }

            subscription.unsubscribe();

        } catch (Exception e) {
            logger.error("An unexpected error occurred", e);
            throw new RuntimeException(e);
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
