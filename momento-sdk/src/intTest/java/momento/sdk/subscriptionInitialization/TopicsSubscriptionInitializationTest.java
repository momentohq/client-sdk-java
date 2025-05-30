package momento.sdk.subscriptionInitialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import momento.sdk.ISubscriptionCallbacks;
import momento.sdk.TopicClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfiguration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.StaticTransportStrategy;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

/******************************************************************/
/* Do not run these tests in CI as they rely on using a cache with
/* a subscription limit >= 2010.
/* Provide the name of your dev cache with greater subscription
/* limits using the TEST_CACHE_NAME environment variable.
/******************************************************************/

public class TopicsSubscriptionInitializationTest {
  private static String cacheName;
  private static CredentialProvider credentialProvider;
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

  @BeforeAll
  static void setupAll() {
    cacheName = System.getenv("TEST_CACHE_NAME");
    if (cacheName == null) {
      throw new RuntimeException("TEST_CACHE_NAME environment variable not set");
    }

    credentialProvider = CredentialProvider.fromEnvVar("MOMENTO_API_KEY");
  }

  @Test
  @Timeout(1000)
  public void oneStreamChannel_doesNotSilentlyQueueSubscribeRequestOnFullChannel() {
    unsubscribeCounter = 0;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(1);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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

    // Ending some subscriptions should free up streams and allow new subscriptions
    subscriptions.get(0).unsubscribe();
    // Wait for the subscription to end
    try {
      Thread.sleep(200);
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
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void twoStreamChannels_handlesBurstOfSubscribeAndUnsubscribeRequests() {
    unsubscribeCounter = 0;
    final int numGrpcChannels = 2;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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
      Thread.sleep(200);
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
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void twoStreamChannels_handlesBurstOfSubscribeRequestsAtHalfOfMaxCapacity() {
    final int numGrpcChannels = 2;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void twoStreamChannels_handlesBurstOfSubscribeRequestsAtMaxCapacity() {
    final int numGrpcChannels = 2;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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

    // Cleanup
    for (TopicSubscribeResponse.Subscription sub : subscriptions) {
      sub.unsubscribe();
    }
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void twoStreamChannels_handlesBurstOfSubscribeRequestsAtOverMaxCapacity() {
    final int numGrpcChannels = 2;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void tenStreamChannels_handlesBurstOfSubscribeRequestsAtHalfOfMaxCapacity() {
    final int numGrpcChannels = 10;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void tenStreamChannels_handlesBurstOfSubscribeRequestsAtMaxCapacity() {
    final int numGrpcChannels = 10;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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

    // Cleanup
    for (TopicSubscribeResponse.Subscription sub : subscriptions) {
      sub.unsubscribe();
    }
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void tenStreamChannels_handlesBurstOfSubscribeRequestsAtOverMaxCapacity() {
    final int numGrpcChannels = 10;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void twentyStreamChannels_handlesBurstOfSubscribeRequestsAtMaxCapacity() {
    final int numGrpcChannels = 20;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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

    // Cleanup
    for (TopicSubscribeResponse.Subscription sub : subscriptions) {
      sub.unsubscribe();
    }
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void twentyStreamChannels_handlesBurstOfSubscribeRequestsAtOverMaxCapacity() {
    final int numGrpcChannels = 20;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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
    topicClient.close();
  }

  @Test
  @Timeout(1000)
  public void shouldDecrementActiveSubscriptionsCountWhenSubscribeRequestsFail() {
    final int numGrpcChannels = 1;
    final int maxStreamCapacity = 100 * numGrpcChannels;
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withNumStreamGrpcChannels(numGrpcChannels);
    final TopicConfiguration topicConfiguration =
        new TopicConfiguration(
            new StaticTransportStrategy(grpcConfig),
            LoggerFactory.getLogger(TopicsSubscriptionInitializationTest.class));
    TopicClient topicClient = TopicClient.builder(credentialProvider, topicConfiguration).build();

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

    // Should successfully start the maximum number of subscriptions because 10 attempts ran into
    // NOT_FOUND_ERROR and the errors should have decremented the active subscriptions count.
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
    topicClient.close();
  }
}
