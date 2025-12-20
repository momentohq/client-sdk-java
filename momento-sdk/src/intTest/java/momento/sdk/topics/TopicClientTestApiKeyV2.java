package momento.sdk.topics;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import momento.sdk.ISubscriptionCallbacks;
import momento.sdk.TopicClient;
import momento.sdk.cache.BaseCacheTestClass;
import momento.sdk.config.TopicConfigurations;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TopicClientTestApiKeyV2 extends BaseCacheTestClass {

  private static TopicClient topicClient;

  @BeforeAll
  static void setupAll() {
    topicClient =
        TopicClient.builder(credentialProviderApiKeyV2, TopicConfigurations.Laptop.latest())
            .build();
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
