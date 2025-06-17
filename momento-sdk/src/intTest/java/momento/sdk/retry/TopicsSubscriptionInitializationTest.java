package momento.sdk.retry;

import static momento.sdk.retry.BaseMomentoLocalTestClass.withCacheAndTopicClient;
import static momento.sdk.retry.BaseMomentoLocalTestClass.withCacheAndTopicClientWithNumStreamChannels;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import momento.sdk.ISubscriptionCallbacks;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import momento.sdk.retry.utils.MomentoLocalMiddlewareArgs;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;

public class TopicsSubscriptionInitializationTest {
  private int unsubscribeCounter = 0;

  private ISubscriptionCallbacks callbacks() {
    return new ISubscriptionCallbacks() {
      @Override
      public void onItem(TopicMessage message) {}

      @Override
      public void onCompleted() {
        unsubscribeCounter++;
      }

      @Override
      public void onError(Throwable t) {}
    };
  }

  private static Logger logger;

  @BeforeAll
  static void setup() {
    logger = getLogger(TopicsSubscriptionInitializationTest.class);
  }

  @Test
  @Timeout(30)
  public void staticPool_oneStreamChannel_doesNotSilentlyQueueSubscribeRequestOnFullChannel()
      throws Exception {
    unsubscribeCounter = 0;

    withCacheAndTopicClientWithNumStreamChannels(
        1,
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          // These should all succeed
          // Starting 100 subscriptions on 1 channel should be fine
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          for (int i = 0; i < 100; i++) {
            final TopicSubscribeResponse response =
                topicClient.subscribe(cacheName, "test-topic", callbacks()).join();
            assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
            subscriptions.add((TopicSubscribeResponse.Subscription) response);
          }

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Starting one more subscription should produce resource exhausted error
          final TopicSubscribeResponse response =
              topicClient.subscribe(cacheName, "test-topic", callbacks()).join();
          assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
          assertEquals(
              MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED,
              ((TopicSubscribeResponse.Error) response).getErrorCode());

          // Ending a subscription should free up one new stream
          subscriptions.get(0).unsubscribe();
          // Wait for the subscription to end
          try {
            Thread.sleep(750);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }
          assertEquals(1, unsubscribeCounter);

          final TopicSubscribeResponse response2 =
              topicClient.subscribe(cacheName, "test-topic", callbacks()).join();
          assertThat(response2).isInstanceOf(TopicSubscribeResponse.Subscription.class);
          subscriptions.add((TopicSubscribeResponse.Subscription) response2);

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            if (sub != null) {
              sub.unsubscribe();
            }
          }
        });
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 10, 20})
  @Timeout(30)
  public void staticPool_multipleStreamChannels_handlesBurstOfSubscribeAndUnsubscribeRequests(
      int numGrpcChannels) throws Exception {
    unsubscribeCounter = 0;
    final int maxStreamCapacity = 100 * numGrpcChannels;

    withCacheAndTopicClientWithNumStreamChannels(
        numGrpcChannels,
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests = new ArrayList<>();
          for (int i = 0; i < maxStreamCapacity; i++) {
            final CompletableFuture<TopicSubscribeResponse> response =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests.add(response);
          }
          // Wait for all the subscribe requests to complete
          CompletableFuture.allOf(subscribeRequests.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Verify they all succeeded
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests) {
            TopicSubscribeResponse response = future.join();
            assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
            subscriptions.add((TopicSubscribeResponse.Subscription) response);
          }

          // Unsubscribe half of the subscriptions
          final int unsubscribeBurstSize = maxStreamCapacity / 2;
          for (int i = 0; i < unsubscribeBurstSize; i++) {
            subscriptions.get(i).unsubscribe();
          }
          // Wait a bit for the subscription to end
          try {
            Thread.sleep(750);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }
          assertEquals(unsubscribeBurstSize, unsubscribeCounter);

          // Burst of subscribe requests should succeed
          final int subscribeBurstSize = maxStreamCapacity / 2 + 10;
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests2 = new ArrayList<>();
          for (int i = 0; i < subscribeBurstSize; i++) {
            final CompletableFuture<TopicSubscribeResponse> subscribePromise =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests2.add(subscribePromise);
          }
          CompletableFuture.allOf(subscribeRequests2.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          List<TopicSubscribeResponse.Subscription> successfulSubscriptions2 = new ArrayList<>();
          int numFailedSubscriptions = 0;
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests2) {
            TopicSubscribeResponse response = future.join();
            if (response instanceof TopicSubscribeResponse.Subscription) {
              successfulSubscriptions2.add((TopicSubscribeResponse.Subscription) response);
            } else {
              numFailedSubscriptions++;
            }
          }
          assertEquals(10, numFailedSubscriptions);
          assertEquals(subscribeBurstSize - 10, successfulSubscriptions2.size());

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 10, 20})
  @Timeout(30)
  public void staticPool_multipleStreamChannels_handlesBurstOfSubscribeRequestsAtMaxCapacity(
      int numGrpcChannels) throws Exception {
    final int maxStreamCapacity = 100 * numGrpcChannels;

    withCacheAndTopicClientWithNumStreamChannels(
        numGrpcChannels,
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests = new ArrayList<>();
          for (int i = 0; i < maxStreamCapacity; i++) {
            final CompletableFuture<TopicSubscribeResponse> response =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests.add(response);
          }
          // Wait for all the subscribe requests to complete
          CompletableFuture.allOf(subscribeRequests.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(750);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Verify they all succeeded
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests) {
            TopicSubscribeResponse response = future.join();
            assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
            subscriptions.add((TopicSubscribeResponse.Subscription) response);
          }

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 10, 20})
  @Timeout(30)
  public void staticPool_multipleStreamChannels_handlesBurstOfSubscribeRequestsAtOverMaxCapacity(
      int numGrpcChannels) throws Exception {
    final int maxStreamCapacity = 100 * numGrpcChannels;

    withCacheAndTopicClientWithNumStreamChannels(
        numGrpcChannels,
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests = new ArrayList<>();
          for (int i = 0; i < maxStreamCapacity + 10; i++) {
            final CompletableFuture<TopicSubscribeResponse> response =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests.add(response);
          }
          // Wait for all the subscribe requests to complete
          CompletableFuture.allOf(subscribeRequests.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Verify they all succeeded
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          int numFailedSubscriptions = 0;
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests) {
            TopicSubscribeResponse response = future.join();
            if (response instanceof TopicSubscribeResponse.Error) {
              numFailedSubscriptions++;
            } else {
              assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
              subscriptions.add((TopicSubscribeResponse.Subscription) response);
            }
          }
          assertEquals(10, numFailedSubscriptions);
          assertEquals(maxStreamCapacity, subscriptions.size());

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 10, 20})
  @Timeout(30)
  public void staticPool_multipleStreamChannels_handlesBurstOfSubscribeRequestsAtHalfOfMaxCapacity(
      int numGrpcChannels) throws Exception {
    final int maxStreamCapacity = 100 * numGrpcChannels;

    withCacheAndTopicClientWithNumStreamChannels(
        numGrpcChannels,
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests = new ArrayList<>();
          for (int i = 0; i < maxStreamCapacity / 2; i++) {
            final CompletableFuture<TopicSubscribeResponse> response =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests.add(response);
          }
          // Wait for all the subscribe requests to complete
          CompletableFuture.allOf(subscribeRequests.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Verify they all succeeded
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests) {
            TopicSubscribeResponse response = future.join();
            assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
            subscriptions.add((TopicSubscribeResponse.Subscription) response);
          }

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @Test
  @Timeout(30)
  public void staticPool_shouldDecrementActiveSubscriptionsCountWhenSubscribeRequestsFail()
      throws Exception {
    final int numGrpcChannels = 1;
    final int maxStreamCapacity = 100 * numGrpcChannels;

    withCacheAndTopicClientWithNumStreamChannels(
        numGrpcChannels,
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          final Semaphore errorSemaphore = new Semaphore(0);
          final AtomicInteger errorCounter = new AtomicInteger(0);

          final ISubscriptionCallbacks callbacks =
              new ISubscriptionCallbacks() {
                @Override
                public void onItem(TopicMessage message) {}

                @Override
                public void onCompleted() {}

                @Override
                public void onError(Throwable t) {
                  errorCounter.incrementAndGet();
                  errorSemaphore.release();
                }
              };

          // Should successfully start the maximum number of subscriptions because 10 attempts
          // ran into NOT_FOUND_ERROR. The errors should have decremented the active subscriptions
          // count.
          List<TopicSubscribeResponse.Subscription> successfulSubscriptions = new ArrayList<>();
          for (int i = 0; i < maxStreamCapacity + 10; i++) {
            String cacheNameToUse = cacheName;
            if (i % 11 == 0) {
              cacheNameToUse = "this-cache-does-not-exist";
            }
            TopicSubscribeResponse attempt =
                topicClient.subscribe(cacheNameToUse, "test-topic", callbacks).join();
            if (attempt instanceof TopicSubscribeResponse.Subscription) {
              successfulSubscriptions.add((TopicSubscribeResponse.Subscription) attempt);
            } else {
              assertThat(attempt).isInstanceOf(TopicSubscribeResponse.Error.class);
              assertThat(((TopicSubscribeResponse.Error) attempt).getErrorCode())
                  .isEqualTo(MomentoErrorCode.NOT_FOUND_ERROR);
              errorCounter.incrementAndGet();
            }
          }

          // Assert that we have received maxStreamCapacity number of successful subscriptions
          assertThat(successfulSubscriptions.size()).isEqualTo(maxStreamCapacity);

          // Assert that we have received 10 NOT_FOUND_ERRORs
          assertThat(errorCounter).hasValue(10);

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : successfulSubscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @Test
  @Timeout(30)
  public void staticPool_oneStreamChannel_properlyDecrementsWhenErrorOccursMidStream()
      throws Exception {
    unsubscribeCounter = 0;
    final AtomicInteger unsubscribeOnErrorCounter = new AtomicInteger(0);
    final ISubscriptionCallbacks callbacks =
        new ISubscriptionCallbacks() {
          @Override
          public void onItem(TopicMessage message) {}

          @Override
          public void onCompleted() {
            System.out.println("onCompleted");
            unsubscribeCounter++;
          }

          @Override
          public void onError(Throwable t) {
            System.out.println("onError");
            unsubscribeOnErrorCounter.incrementAndGet();
          }
        };

    final MomentoLocalMiddlewareArgs middlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .streamError(MomentoErrorCode.NOT_FOUND_ERROR)
            .streamErrorRpcList(Collections.singletonList(MomentoRpcMethod.TOPIC_SUBSCRIBE))
            .streamErrorMessageLimit(3)
            .build();

    withCacheAndTopicClientWithNumStreamChannels(
        1,
        middlewareArgs,
        (topicClient, cacheName) -> {
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();

          // Subscribe but expecting an error after a couple of heartbeats
          final TopicSubscribeResponse response =
              topicClient.subscribe(cacheName, "topic", callbacks).join();
          assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
          subscriptions.add((TopicSubscribeResponse.Subscription) response);

          // Wait for the subscription that ran into the error to be closed
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            if (sub != null) {
              sub.unsubscribe();
            }
          }

          assertEquals(0, unsubscribeCounter);
          assertEquals(1, unsubscribeOnErrorCounter.get());
        });
  }

  @Test
  @Timeout(30)
  public void dynamicPool_oneStreamChannel_doesNotSilentlyQueueSubscribeRequestOnFullChannel()
      throws Exception {
    unsubscribeCounter = 0;

    withCacheAndTopicClient(
        (config) -> config.withMaxSubscriptions(100),
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          // These should all succeed
          // Starting 100 subscriptions on 1 channel should be fine
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          for (int i = 0; i < 100; i++) {
            final TopicSubscribeResponse response =
                topicClient.subscribe(cacheName, "test-topic", callbacks()).join();
            assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
            subscriptions.add((TopicSubscribeResponse.Subscription) response);
          }

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Starting one more subscription should produce resource exhausted error
          final TopicSubscribeResponse response =
              topicClient.subscribe(cacheName, "test-topic", callbacks()).join();
          assertThat(response).isInstanceOf(TopicSubscribeResponse.Error.class);
          assertEquals(
              MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED,
              ((TopicSubscribeResponse.Error) response).getErrorCode());

          // Ending a subscription should free up one new stream
          subscriptions.get(0).unsubscribe();
          // Wait for the subscription to end
          try {
            Thread.sleep(750);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }
          assertEquals(1, unsubscribeCounter);

          final TopicSubscribeResponse response2 =
              topicClient.subscribe(cacheName, "test-topic", callbacks()).join();
          assertThat(response2).isInstanceOf(TopicSubscribeResponse.Subscription.class);
          subscriptions.add((TopicSubscribeResponse.Subscription) response2);

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            if (sub != null) {
              sub.unsubscribe();
            }
          }
        });
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 1000, 2000})
  @Timeout(60)
  public void dynamicPool_multipleStreamChannels_handlesBurstOfSubscribeAndUnsubscribeRequests(
      int maxSubscriptions) throws Exception {
    unsubscribeCounter = 0;

    withCacheAndTopicClient(
        (config) -> config.withMaxSubscriptions(maxSubscriptions),
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests = new ArrayList<>();
          for (int i = 0; i < maxSubscriptions; i++) {
            final CompletableFuture<TopicSubscribeResponse> response =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests.add(response);
          }
          // Wait for all the subscribe requests to complete
          CompletableFuture.allOf(subscribeRequests.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Verify they all succeeded
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests) {
            TopicSubscribeResponse response = future.join();
            assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
            subscriptions.add((TopicSubscribeResponse.Subscription) response);
          }

          // Unsubscribe half of the subscriptions
          final int unsubscribeBurstSize = maxSubscriptions / 2;
          for (int i = 0; i < unsubscribeBurstSize; i++) {
            subscriptions.get(i).unsubscribe();
          }
          // Wait a bit for the subscription to end
          try {
            Thread.sleep(750);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }
          assertEquals(unsubscribeBurstSize, unsubscribeCounter);

          // Burst of subscribe requests should succeed
          final int subscribeBurstSize = maxSubscriptions / 2 + 10;
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests2 = new ArrayList<>();
          for (int i = 0; i < subscribeBurstSize; i++) {
            final CompletableFuture<TopicSubscribeResponse> subscribePromise =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests2.add(subscribePromise);
          }
          CompletableFuture.allOf(subscribeRequests2.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          List<TopicSubscribeResponse.Subscription> successfulSubscriptions2 = new ArrayList<>();
          int numFailedSubscriptions = 0;
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests2) {
            TopicSubscribeResponse response = future.join();
            if (response instanceof TopicSubscribeResponse.Subscription) {
              successfulSubscriptions2.add((TopicSubscribeResponse.Subscription) response);
            } else {
              numFailedSubscriptions++;
            }
          }
          assertEquals(10, numFailedSubscriptions);
          assertEquals(subscribeBurstSize - 10, successfulSubscriptions2.size());

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 1000, 2000})
  @Timeout(60)
  public void dynamicPool_multipleStreamChannels_handlesBurstOfSubscribeRequestsAtMaxCapacity(
      int maxSubscriptions) throws Exception {

    withCacheAndTopicClient(
        (config) -> config.withMaxSubscriptions(maxSubscriptions),
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests = new ArrayList<>();
          for (int i = 0; i < maxSubscriptions; i++) {
            final CompletableFuture<TopicSubscribeResponse> response =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests.add(response);
          }
          // Wait for all the subscribe requests to complete
          CompletableFuture.allOf(subscribeRequests.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Verify they all succeeded
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests) {
            TopicSubscribeResponse response = future.join();
            assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
            subscriptions.add((TopicSubscribeResponse.Subscription) response);
          }

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 1000, 2000})
  @Timeout(60)
  public void dynamicPool_multipleStreamChannels_handlesBurstOfSubscribeRequestsAtOverMaxCapacity(
      int maxSubscriptions) throws Exception {

    withCacheAndTopicClient(
        (config) -> config.withMaxSubscriptions(maxSubscriptions),
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests = new ArrayList<>();
          for (int i = 0; i < maxSubscriptions + 10; i++) {
            final CompletableFuture<TopicSubscribeResponse> response =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests.add(response);
          }
          // Wait for all the subscribe requests to complete
          CompletableFuture.allOf(subscribeRequests.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Verify they all succeeded
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          int numFailedSubscriptions = 0;
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests) {
            TopicSubscribeResponse response = future.join();
            if (response instanceof TopicSubscribeResponse.Error) {
              numFailedSubscriptions++;
            } else {
              assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
              subscriptions.add((TopicSubscribeResponse.Subscription) response);
            }
          }
          assertEquals(10, numFailedSubscriptions);
          assertEquals(maxSubscriptions, subscriptions.size());

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 1000, 2000})
  @Timeout(60)
  public void dynamicPool_multipleStreamChannels_handlesBurstOfSubscribeRequestsAtHalfOfMaxCapacity(
      int maxSubscriptions) throws Exception {

    withCacheAndTopicClient(
        (config) -> config.withMaxSubscriptions(maxSubscriptions),
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          List<CompletableFuture<TopicSubscribeResponse>> subscribeRequests = new ArrayList<>();
          for (int i = 0; i < maxSubscriptions / 2; i++) {
            final CompletableFuture<TopicSubscribeResponse> response =
                topicClient.subscribe(cacheName, "test-topic", callbacks());
            subscribeRequests.add(response);
          }
          // Wait for all the subscribe requests to complete
          CompletableFuture.allOf(subscribeRequests.toArray(new CompletableFuture[0])).join();

          // Wait a bit for all subscriptions to be fully established
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Verify they all succeeded
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();
          for (CompletableFuture<TopicSubscribeResponse> future : subscribeRequests) {
            TopicSubscribeResponse response = future.join();
            assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
            subscriptions.add((TopicSubscribeResponse.Subscription) response);
          }

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @Test
  @Timeout(30)
  public void dynamicPool_shouldDecrementActiveSubscriptionsCountWhenSubscribeRequestsFail()
      throws Exception {
    final int maxSubscriptions = 100;

    withCacheAndTopicClient(
        (config) -> config.withMaxSubscriptions(maxSubscriptions),
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build(),
        (topicClient, cacheName) -> {
          final Semaphore errorSemaphore = new Semaphore(0);
          final AtomicInteger errorCounter = new AtomicInteger(0);

          final ISubscriptionCallbacks callbacks =
              new ISubscriptionCallbacks() {
                @Override
                public void onItem(TopicMessage message) {}

                @Override
                public void onCompleted() {}

                @Override
                public void onError(Throwable t) {
                  errorCounter.incrementAndGet();
                  errorSemaphore.release();
                }
              };

          // Should successfully start the maximum number of subscriptions because 10 attempts ran
          // into NOT_FOUND_ERROR. The errors should have decremented the active subscriptions
          // count.
          List<TopicSubscribeResponse.Subscription> successfulSubscriptions = new ArrayList<>();
          for (int i = 0; i < maxSubscriptions + 10; i++) {
            String cacheNameToUse = cacheName;
            if (i % 11 == 0) {
              cacheNameToUse = "this-cache-does-not-exist";
            }
            TopicSubscribeResponse attempt =
                topicClient.subscribe(cacheNameToUse, "test-topic", callbacks).join();
            if (attempt instanceof TopicSubscribeResponse.Subscription) {
              successfulSubscriptions.add((TopicSubscribeResponse.Subscription) attempt);
            } else {
              assertThat(attempt).isInstanceOf(TopicSubscribeResponse.Error.class);
              assertThat(((TopicSubscribeResponse.Error) attempt).getErrorCode())
                  .isEqualTo(MomentoErrorCode.NOT_FOUND_ERROR);
              errorCounter.incrementAndGet();
            }
          }

          // Assert that we have received maxStreamCapacity number of successful subscriptions
          assertThat(successfulSubscriptions.size()).isEqualTo(maxSubscriptions);

          // Assert that we have received 10 NOT_FOUND_ERRORs
          assertThat(errorCounter).hasValue(10);

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : successfulSubscriptions) {
            sub.unsubscribe();
          }
        });
  }

  @Test
  @Timeout(30)
  public void dynamicPool_oneStreamChannel_properlyDecrementsWhenErrorOccursMidStream()
      throws Exception {
    unsubscribeCounter = 0;
    final AtomicInteger unsubscribeOnErrorCounter = new AtomicInteger(0);
    final ISubscriptionCallbacks callbacks =
        new ISubscriptionCallbacks() {
          @Override
          public void onItem(TopicMessage message) {}

          @Override
          public void onCompleted() {
            System.out.println("onCompleted");
            unsubscribeCounter++;
          }

          @Override
          public void onError(Throwable t) {
            System.out.println("onError");
            unsubscribeOnErrorCounter.incrementAndGet();
          }
        };

    final MomentoLocalMiddlewareArgs middlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .streamError(MomentoErrorCode.NOT_FOUND_ERROR)
            .streamErrorRpcList(Collections.singletonList(MomentoRpcMethod.TOPIC_SUBSCRIBE))
            .streamErrorMessageLimit(3)
            .build();

    withCacheAndTopicClient(
        (config) -> config.withMaxSubscriptions(100),
        middlewareArgs,
        (topicClient, cacheName) -> {
          List<TopicSubscribeResponse.Subscription> subscriptions = new ArrayList<>();

          // Subscribe but expecting an error after a couple of heartbeats
          final TopicSubscribeResponse response =
              topicClient.subscribe(cacheName, "topic", callbacks).join();
          assertThat(response).isInstanceOf(TopicSubscribeResponse.Subscription.class);
          subscriptions.add((TopicSubscribeResponse.Subscription) response);

          // Wait for the subscription that ran into the error to be closed
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted while waiting for subscriptions", e);
          }

          // Cleanup
          for (TopicSubscribeResponse.Subscription sub : subscriptions) {
            if (sub != null) {
              sub.unsubscribe();
            }
          }

          assertEquals(0, unsubscribeCounter);
          assertEquals(1, unsubscribeOnErrorCounter.get());
        });
  }
}
