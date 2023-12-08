package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import momento.sdk.config.Configurations;
import momento.sdk.config.TopicConfigurations;
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
  CountDownLatch latch = new CountDownLatch(1);
  private final Logger logger = LoggerFactory.getLogger(SubscriptionWrapper.class);

  private final List<String> receivedStringValues = new ArrayList<>();
  private final List<byte[]> receivedByteArrayValues = new ArrayList<>();
  ISubscribeCallOptions options =
      new ISubscribeCallOptions() {
        @Override
        public void onItem(TopicMessage message) {
          logger.info("onItem Invoked");
          logger.info(message.toString());
          if (message instanceof TopicMessage.Text) {
            receivedStringValues.add(((TopicMessage.Text) message).getValue());
          } else if (message instanceof TopicMessage.Binary) {
            receivedByteArrayValues.add(((TopicMessage.Binary) message).getValue());
          }
          latch.countDown();
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

    topicClient =
        TopicClient.builder(credentialProvider, TopicConfigurations.Laptop.latest()).build();
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

    assertTrue(latch.await(5, TimeUnit.SECONDS));

    List<byte[]> expectedReceivedValues = new ArrayList<>();
    expectedReceivedValues.add(value);

    assertArrayEquals(
        expectedReceivedValues.toArray(new byte[0][]),
        receivedByteArrayValues.toArray(new byte[0][]),
        "Received values do not match the expected values");

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

    assertTrue(latch.await(5, TimeUnit.SECONDS));

    List<String> expectedReceivedValues = new ArrayList<>();
    expectedReceivedValues.add(value);

    assertEquals(expectedReceivedValues, receivedStringValues);

    ((TopicSubscribeResponse.Subscription) response).unsubscribe();
  }
}
