package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.MomentoErrorCode;
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
  public void topicPublishNullChecksIsError() {
    byte[] byteValue = new byte[0];
    String stringValue = "test-value";

    // badCacheName, validTopicName, byteArray value
    TopicPublishResponse response = topicClient.publish(null, topicName, byteValue).join();
    assertThat(response).isInstanceOf(TopicPublishResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicPublishResponse.Error) response).getErrorCode());

    // badCacheName, validTopicName, string value
    response = topicClient.publish(null, topicName, stringValue).join();
    assertThat(response).isInstanceOf(TopicPublishResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicPublishResponse.Error) response).getErrorCode());

    // validCacheName, badTopicName, byteArray value
    response = topicClient.publish(cacheName, null, byteValue).join();
    assertThat(response).isInstanceOf(TopicPublishResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicPublishResponse.Error) response).getErrorCode());

    // validCacheName, badTopicName, byteArray value
    response = topicClient.publish(cacheName, null, stringValue).join();
    assertThat(response).isInstanceOf(TopicPublishResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicPublishResponse.Error) response).getErrorCode());

    // validCacheName, validTopicName, null byteArray value
    response = topicClient.publish(cacheName, topicName, (byte[]) null).join();
    assertThat(response).isInstanceOf(TopicPublishResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicPublishResponse.Error) response).getErrorCode());

    // validCacheName, validTopicName, null string value
    response = topicClient.publish(cacheName, topicName, (String) null).join();
    assertThat(response).isInstanceOf(TopicPublishResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicPublishResponse.Error) response).getErrorCode());
  }

  @Test
  public void topicSubscribeNullChecksIsError() {
    // badCacheName, validTopicName
    TopicSubscribeResponse response = topicClient.subscribe(null, topicName, options).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicSubscribeResponse.Error) response).getErrorCode());

    // validCacheName, badTopicName
    response = topicClient.subscribe(cacheName, null, options).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicSubscribeResponse.Error) response).getErrorCode());
  }

  @Test
  public void topicPublishSubscribe_ByteArray_HappyPath() throws InterruptedException {

    byte[] value = new byte[] {0x00};

    TopicSubscribeResponse response = topicClient.subscribe(cacheName, topicName, options).join();
    logger.info(response.toString());
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);

    TopicPublishResponse publishResponse = topicClient.publish(cacheName, topicName, value).join();
    logger.info(publishResponse.toString());
    assertThat(publishResponse).isInstanceOf(TopicPublishResponse.Success.class);

    Thread.sleep(1000);
    ((TopicSubscribeResponse.Subscription) response).unsubscribe();
  }

  @Test
  public void topicPublishSubscribe_String_HappyPath() throws InterruptedException {
    String value = "test-value";

    TopicSubscribeResponse response = topicClient.subscribe(cacheName, topicName, options).join();
    logger.info(response.toString());
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);

    TopicPublishResponse publishResponse = topicClient.publish(cacheName, topicName, value).join();
    logger.info(publishResponse.toString());
    assertThat(publishResponse).isInstanceOf(TopicPublishResponse.Success.class);

    Thread.sleep(1000);
    ((TopicSubscribeResponse.Subscription) response).unsubscribe();
  }
}
