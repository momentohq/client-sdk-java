package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import momento.sdk.config.TopicConfigurations;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicClientTest extends BaseCacheTestClass {
  private static TopicClient topicClient;

  private final String topicName = "test-topic";
  private final Logger logger = LoggerFactory.getLogger(SubscriptionWrapper.class);

  private final List<String> receivedStringValues = new ArrayList<>();
  private final List<byte[]> receivedByteArrayValues = new ArrayList<>();

  @BeforeAll
  static void setupAll() {
    topicClient =
        TopicClient.builder(credentialProvider, TopicConfigurations.Laptop.latest()).build();
  }

  @AfterAll
  static void teardownAll() {
    topicClient.close();
  }

  private ISubscriptionCallbacks callbacks(CountDownLatch latch) {
    return new ISubscriptionCallbacks() {
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
  public void topicPublishCacheDoesNotExistIsError() {
    String stringValue = "test-value";
    TopicPublishResponse response =
        topicClient.publish("doesNotExist", topicName, stringValue).join();
    assertThat(response).isInstanceOf(TopicPublishResponse.Error.class);
    assertEquals(
        MomentoErrorCode.NOT_FOUND_ERROR, ((TopicPublishResponse.Error) response).getErrorCode());
  }

  @Test
  public void topicSubscribeNullChecksIsError() {
    // badCacheName, validTopicName
    CountDownLatch latch = new CountDownLatch(1);
    TopicSubscribeResponse response =
        topicClient.subscribe(null, topicName, callbacks(latch)).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicSubscribeResponse.Error) response).getErrorCode());

    // validCacheName, badTopicName
    response = topicClient.subscribe(cacheName, null, callbacks(latch)).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicSubscribeResponse.Error) response).getErrorCode());
  }

  @Test
  public void topicSubscribeCacheDoesNotExistIsError() {
    TopicSubscribeResponse response =
        topicClient.subscribe("doesNotExist", topicName, callbacks(new CountDownLatch(1))).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
    assertEquals(
        MomentoErrorCode.NOT_FOUND_ERROR, ((TopicSubscribeResponse.Error) response).getErrorCode());
  }

  @Test
  public void topicPublishSubscribe_ByteArray_HappyPath() throws InterruptedException {

    byte[] value = new byte[] {0x00};

    CountDownLatch latch = new CountDownLatch(1);
    TopicSubscribeResponse response =
        topicClient.subscribe(cacheName, topicName, callbacks(latch)).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);

    TopicPublishResponse publishResponse = topicClient.publish(cacheName, topicName, value).join();
    assertThat(publishResponse).isInstanceOf(TopicPublishResponse.Success.class);

    latch.await();

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

    CountDownLatch latch = new CountDownLatch(1);
    TopicSubscribeResponse response =
        topicClient.subscribe(cacheName, topicName, callbacks(latch)).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);

    TopicPublishResponse publishResponse = topicClient.publish(cacheName, topicName, value).join();
    assertThat(publishResponse).isInstanceOf(TopicPublishResponse.Success.class);

    latch.await();

    List<String> expectedReceivedValues = new ArrayList<>();
    expectedReceivedValues.add(value);

    logger.info("expectedReceivedValues: " + expectedReceivedValues);
    logger.info("receivedStringValues: " + receivedStringValues);

    assertEquals(expectedReceivedValues, receivedStringValues);

    ((TopicSubscribeResponse.Subscription) response).unsubscribe();
  }
}
