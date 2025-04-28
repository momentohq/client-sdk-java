package momento.sdk.topics;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import momento.sdk.ISubscriptionCallbacks;
import momento.sdk.TopicClient;
import momento.sdk.cache.BaseCacheTestClass;
import momento.sdk.config.TopicConfigurations;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TopicClientTest extends BaseCacheTestClass {
  private static TopicClient topicClient;

  @BeforeAll
  static void setupAll() {
    topicClient =
        TopicClient.builder(credentialProvider, TopicConfigurations.Laptop.latest()).build();
  }

  @AfterAll
  static void teardownAll() {
    topicClient.close();
  }

  private ISubscriptionCallbacks callbacks() {
    return new ISubscriptionCallbacks() {
      @Override
      public void onItem(TopicMessage message) {}

      @Override
      public void onCompleted() {}

      @Override
      public void onError(Throwable t) {}
    };
  }

  private ISubscriptionCallbacks callbacks(
      Semaphore onItemSemaphore, List<TopicMessage> receivedMessages) {
    return new ISubscriptionCallbacks() {
      @Override
      public void onItem(TopicMessage message) {
        receivedMessages.add(message);
        onItemSemaphore.release();
      }

      @Override
      public void onCompleted() {}

      @Override
      public void onError(Throwable t) {}
    };
  }

  @Test
  public void topicPublishNullChecksIsError() {
    final String topicName = randomString();
    final byte[] byteValue = new byte[0];
    final String stringValue = "test-value";

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
    final String topicName = randomString();
    final String stringValue = "test-value";
    final TopicPublishResponse response =
        topicClient.publish("doesNotExist", topicName, stringValue).join();
    assertThat(response).isInstanceOf(TopicPublishResponse.Error.class);
    assertEquals(
        MomentoErrorCode.NOT_FOUND_ERROR, ((TopicPublishResponse.Error) response).getErrorCode());
  }

  @Test
  public void topicSubscribeNullChecksIsError() {
    // badCacheName, validTopicName
    final String topicName = randomString();
    TopicSubscribeResponse response = topicClient.subscribe(null, topicName, callbacks()).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicSubscribeResponse.Error) response).getErrorCode());

    // validCacheName, badTopicName
    response = topicClient.subscribe(cacheName, null, callbacks()).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((TopicSubscribeResponse.Error) response).getErrorCode());
  }

  @Test
  public void topicSubscribeCacheDoesNotExistIsError() {
    final String topicName = randomString();
    final TopicSubscribeResponse response =
        topicClient.subscribe("doesNotExist", topicName, callbacks()).join();
    assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
    assertEquals(
        MomentoErrorCode.NOT_FOUND_ERROR, ((TopicSubscribeResponse.Error) response).getErrorCode());
  }

  @Test
  @Timeout(10)
  public void topicPublishSubscribe_ByteArray_HappyPath() throws InterruptedException {
    final String topicName = randomString();
    final byte[] value = new byte[] {0x00};

    final Semaphore onItemSemaphore = new Semaphore(0);
    final List<TopicMessage> receivedMessages = new ArrayList<>();
    final ISubscriptionCallbacks callbacks = callbacks(onItemSemaphore, receivedMessages);

    final TopicSubscribeResponse subscribeResponse =
        topicClient.subscribe(cacheName, topicName, callbacks).join();
    assertThat(subscribeResponse).isInstanceOf(TopicSubscribeResponse.Subscription.class);

    try {
      final CompletableFuture<TopicPublishResponse> publishFuture =
          topicClient.publish(cacheName, topicName, value);
      onItemSemaphore.acquire();

      assertThat(publishFuture)
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(TopicPublishResponse.Success.class);

      assertThat(receivedMessages)
          .filteredOn(tm -> tm instanceof TopicMessage.Binary)
          .map(tm -> ((TopicMessage.Binary) tm).getValue())
          .containsOnly(value);
    } finally {
      ((TopicSubscribeResponse.Subscription) subscribeResponse).unsubscribe();
    }
  }

  @Test
  @Timeout(10)
  public void topicPublishSubscribe_String_HappyPath() throws InterruptedException {
    final String topicName = randomString();
    final String value = "test-value";

    final Semaphore onItemSemaphore = new Semaphore(0);
    final List<TopicMessage> receivedMessages = new ArrayList<>();
    final ISubscriptionCallbacks callbacks = callbacks(onItemSemaphore, receivedMessages);

    final TopicSubscribeResponse subscribeResponse =
        topicClient.subscribe(cacheName, topicName, callbacks).join();
    assertThat(subscribeResponse).isInstanceOf(TopicSubscribeResponse.Subscription.class);

    try {
      final CompletableFuture<TopicPublishResponse> publishFuture =
          topicClient.publish(cacheName, topicName, value);
      onItemSemaphore.acquire();

      assertThat(publishFuture)
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(TopicPublishResponse.Success.class);

      assertThat(receivedMessages)
          .filteredOn(tm -> tm instanceof TopicMessage.Text)
          .map(tm -> ((TopicMessage.Text) tm).getValue())
          .containsOnly(value);
    } finally {
      ((TopicSubscribeResponse.Subscription) subscribeResponse).unsubscribe();
    }
  }
}
