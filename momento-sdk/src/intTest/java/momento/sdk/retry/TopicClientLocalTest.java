package momento.sdk.retry;

import static momento.sdk.retry.BaseMomentoLocalTestClass.FIVE_SECONDS;
import static momento.sdk.retry.BaseMomentoLocalTestClass.withCacheAndTopicClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import momento.sdk.ISubscriptionCallbacks;
import momento.sdk.TopicClient;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import momento.sdk.retry.utils.MomentoLocalMiddlewareArgs;
import momento.sdk.retry.utils.TestAdminClient;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;

public class TopicClientLocalTest {

  private static Logger logger;

  @BeforeAll
  static void setup() {
    logger = getLogger(TopicClientLocalTest.class);
  }

  @Test
  @Timeout(10)
  void testSubscribe_shouldRetryWithRecoverableError() throws Exception {
    final int streamErrorMessageLimit = 3;
    final MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .streamError(MomentoErrorCode.SERVER_UNAVAILABLE)
            .streamErrorRpcList(Collections.singletonList(MomentoRpcMethod.TOPIC_SUBSCRIBE))
            .streamErrorMessageLimit(streamErrorMessageLimit)
            .build();

    final Semaphore connectionLostSemaphore = new Semaphore(0);
    final Semaphore connectionRestoredSemaphore = new Semaphore(0);

    final AtomicInteger connectionLostCounter = new AtomicInteger(0);
    final AtomicInteger connectionRestoredCounter = new AtomicInteger(0);
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
          }

          @Override
          public void onConnectionLost() {
            connectionLostCounter.incrementAndGet();
            connectionLostSemaphore.release();
          }

          @Override
          public void onConnectionRestored() {
            connectionRestoredCounter.incrementAndGet();
            connectionRestoredSemaphore.release();
          }
        };

    withCacheAndTopicClient(
        config -> config,
        momentoLocalMiddlewareArgs,
        (topicClient, cacheName) -> {
          final String topicName = "topic";

          assertThat(topicClient.subscribe(cacheName, topicName, callbacks))
              .succeedsWithin(FIVE_SECONDS)
              .asInstanceOf(
                  InstanceOfAssertFactories.type(TopicSubscribeResponse.Subscription.class))
              .satisfies(
                  subscription -> {
                    // Publish enough messages to hit the error and then wait for that error
                    publishMessages(topicClient, cacheName, topicName, streamErrorMessageLimit);
                    connectionLostSemaphore.acquire();

                    // Assert that we have lost the connection
                    final int numRetries = connectionLostCounter.get();
                    assertThat(numRetries).isGreaterThan(0);

                    // wait for the connection to be restored, then send more messages to trigger
                    // the error again
                    connectionRestoredSemaphore.acquire();
                    publishMessages(topicClient, cacheName, topicName, streamErrorMessageLimit);
                    connectionLostSemaphore.acquire();

                    assertThat(connectionLostCounter).hasValueGreaterThan(numRetries);
                    assertThat(connectionRestoredCounter).hasValueGreaterThan(0);
                    assertThat(errorCounter).hasValue(0);

                    subscription.unsubscribe();
                  });
        });
  }

  @Test
  @Timeout(10)
  void testSubscribe_shouldNotRetryWithUnrecoverableError() throws Exception {
    final int streamErrorMessageLimit = 3;
    final MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .streamError(MomentoErrorCode.INTERNAL_SERVER_ERROR)
            .streamErrorRpcList(Collections.singletonList(MomentoRpcMethod.TOPIC_SUBSCRIBE))
            .streamErrorMessageLimit(streamErrorMessageLimit)
            .build();

    final Semaphore errorSemaphore = new Semaphore(0);

    final AtomicInteger connectionLostCounter = new AtomicInteger(0);
    final AtomicInteger connectionRestoredCounter = new AtomicInteger(0);
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

          @Override
          public void onConnectionLost() {
            connectionLostCounter.incrementAndGet();
          }

          @Override
          public void onConnectionRestored() {
            connectionRestoredCounter.incrementAndGet();
          }
        };

    withCacheAndTopicClient(
        config -> config,
        momentoLocalMiddlewareArgs,
        (topicClient, cacheName) -> {
          final String topicName = "topic";

          assertThat(topicClient.subscribe(cacheName, topicName, callbacks))
              .succeedsWithin(FIVE_SECONDS)
              .asInstanceOf(
                  InstanceOfAssertFactories.type(TopicSubscribeResponse.Subscription.class))
              .satisfies(
                  subscription -> {
                    // Publish enough messages to hit the error and then wait for that error
                    publishMessages(topicClient, cacheName, topicName, streamErrorMessageLimit);
                    errorSemaphore.acquire();

                    // Assert that we have lost the connection
                    assertThat(errorCounter).hasValueGreaterThan(0);
                    assertThat(connectionLostCounter).hasValueGreaterThan(0);
                    assertThat(connectionRestoredCounter).hasValue(0);
                  });
        });
  }

  @Test
  @Timeout(10)
  void testPublish_errorOnDeadlineExceeded() throws Exception {
    final MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString())
            .delayRpcList(Collections.singletonList(MomentoRpcMethod.TOPIC_PUBLISH))
            .delayMillis(100)
            .build();

    withCacheAndTopicClient(
        config -> config.withTimeout(Duration.ofMillis(50)),
        momentoLocalMiddlewareArgs,
        (topicClient, cacheName) ->
            assertThat(topicClient.publish(cacheName, "topic", "message"))
                .succeedsWithin(FIVE_SECONDS)
                .asInstanceOf(InstanceOfAssertFactories.type(TopicPublishResponse.Error.class))
                .extracting(SdkException::getErrorCode)
                .isEqualTo(MomentoErrorCode.TIMEOUT_ERROR));
  }

  @Test
  @Timeout(30)
  void testTestAdmin_pauseSubscriptionOnPortBlockAndResumeSubscriptionOnPortUnblock()
      throws Exception {
    final TestAdminClient testAdminClient = new TestAdminClient();
    final MomentoLocalMiddlewareArgs momentoLocalMiddlewareArgs =
        new MomentoLocalMiddlewareArgs.Builder(logger, UUID.randomUUID().toString()).build();

    final Semaphore heartbeatSemaphore = new Semaphore(0);
    final Semaphore lostConnectionSemaphore = new Semaphore(0);
    final Semaphore restoredConnectionSemaphore = new Semaphore(0);

    final AtomicInteger connectionLostCounter = new AtomicInteger(0);
    final AtomicInteger connectionRestoredCounter = new AtomicInteger(0);
    final AtomicInteger heartbeatCounter = new AtomicInteger(0);
    final int noOfHeartbeatsBeforeSemaphoreRelease = 3;

    final ISubscriptionCallbacks callbacks =
        new ISubscriptionCallbacks() {
          @Override
          public void onItem(TopicMessage message) {}

          @Override
          public void onCompleted() {}

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onConnectionLost() {
            connectionLostCounter.incrementAndGet();
            lostConnectionSemaphore.release();
          }

          @Override
          public void onConnectionRestored() {
            connectionRestoredCounter.incrementAndGet();
            restoredConnectionSemaphore.release();
          }

          @Override
          public void onHeartbeat() {
              // Release the semaphore after multiple heartbeats
              if (heartbeatCounter.incrementAndGet() >= noOfHeartbeatsBeforeSemaphoreRelease) {
                  heartbeatSemaphore.release();
              }
          }
        };

    withCacheAndTopicClient(
        config -> config,
        momentoLocalMiddlewareArgs,
        (topicClient, cacheName) -> {
          final String topicName = "topic";

          assertThat(topicClient.subscribe(cacheName, topicName, callbacks))
              .succeedsWithin(FIVE_SECONDS)
              .asInstanceOf(
                  InstanceOfAssertFactories.type(TopicSubscribeResponse.Subscription.class))
              .satisfies(
                  subscription -> {
                    // Wait for the first heartbeats to confirm connection
                    heartbeatSemaphore.acquire();
                    int initialHeartbeatCount = heartbeatCounter.get();
                    assertThat(initialHeartbeatCount).isGreaterThan(0);

                    // Block the admin port
                    testAdminClient.blockPort();
                    // Wait for connection lost event
                    lostConnectionSemaphore.acquire();
                    assertThat(connectionLostCounter.get()).isGreaterThan(0);

                    // Ensure heartbeats are paused
                    int heartbeatsAfterBlock = heartbeatCounter.get();
                    assertThat(heartbeatsAfterBlock).isEqualTo(initialHeartbeatCount);

                    // Unblock the admin port
                    testAdminClient.unblockPort();

                    // Wait for connection restoration
                    restoredConnectionSemaphore.acquire();
                    assertThat(connectionRestoredCounter.get()).isGreaterThan(0);

                    // Ensure heartbeats resume
                    // wait for the heartbeat to be received
                    heartbeatSemaphore.acquire();
                    int heartbeatsAfterUnblock = heartbeatCounter.get();
                    assertThat(heartbeatsAfterUnblock).isGreaterThan(initialHeartbeatCount);

                    subscription.unsubscribe();
                  });
        });
  }

  private void publishMessages(
      TopicClient client, String cacheName, String topicName, int numMessages) {
    for (int i = 0; i < numMessages; i++) {
      client.publish(cacheName, topicName, "message" + i);
    }
  }
}
