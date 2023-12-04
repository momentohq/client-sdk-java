package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import momento.sdk.config.Configurations;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicClientTest extends BaseTestClass {
  private static final Duration DEFAULT_TTL = Duration.ofSeconds(60);

  private final String cacheName = System.getenv("TEST_CACHE_NAME");
  private CacheClient cacheClient;
  private TopicClient topicClient;

  private final String topicName = "test-topic";
  private final Logger logger = LoggerFactory.getLogger(SubscriptionWrapper.class);

  @BeforeEach
  void setup() {
    cacheClient =
        CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL)
            .build();

    topicClient = TopicClient.builder(credentialProvider, Configurations.Laptop.latest()).build();
    cacheClient.createCache(cacheName).join();
  }

  @AfterEach
  void teardown() {
    cacheClient.deleteCache(cacheName).join();
    cacheClient.close();
    topicClient.close();
  }

  @Test
  public void topicPublish() {
    String value = "test-value";
    TopicPublishResponse response = topicClient.publish(cacheName, topicName, value).join();
    assertThat(response).isInstanceOf(TopicPublishResponse.Success.class);
  }

  @Test
  public void topicSubscribe() throws InterruptedException {
    ISubscribeCallOptions options =
        new ISubscribeCallOptions() {
          @Override
          public void onItem(TopicMessage message) {
            logger.info("onItem Invoked");
            logger.info(message.toString());
          }

          @Override
          public void onCompleted() {
            logger.info("onCompleted Invoked");
          }

          @Override
          public void onError(Throwable t) {
            logger.info("onError Invoked");
          }
        };

    TopicSubscribeResponse response =
        topicClient.subscribe("test-cache", topicName, options).join();
    logger.info(response.toString());
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);

    TopicPublishResponse publishResponse =
        topicClient.publish("test-cache", topicName, "test-value").join();
    logger.info(publishResponse.toString());
    assertThat(publishResponse).isInstanceOf(TopicPublishResponse.Success.class);

    Thread.sleep(1000);
    ((TopicSubscribeResponse.Subscription) response).unsubscribe();
  }
}
