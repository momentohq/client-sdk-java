package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import grpc.cache_client.pubsub._Heartbeat;
import grpc.cache_client.pubsub._SubscriptionItem;
import grpc.cache_client.pubsub._SubscriptionRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import momento.sdk.internal.SubscriptionState;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.retry.FixedDelaySubscriptionRetryStrategy;
import momento.sdk.retry.SubscriptionRetryStrategy;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionWrapperTest {
  private final Logger logger = LoggerFactory.getLogger(SubscriptionWrapperTest.class);
  private final long requestTimeoutSeconds = 5;

  @Test
  public void testConnectionLostAndRestored() throws InterruptedException {
    final String cacheName = "cache";
    final String topicName = "topic";
    final SubscriptionState state = new SubscriptionState();

    final AtomicBoolean gotConnectionLostCallback = new AtomicBoolean(false);
    final AtomicBoolean gotConnectionRestoredCallback = new AtomicBoolean(false);

    final Semaphore waitingForSubscriptionAttempt = new Semaphore(0);

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
            logger.info("Got to our connection lost callback!");
            gotConnectionLostCallback.set(true);
          }

          @Override
          public void onConnectionRestored() {
            logger.info("Got to our connection restored callback!");
            gotConnectionRestoredCallback.set(true);
          }
        };

    final IScsTopicConnection connection =
        new IScsTopicConnection() {
          boolean isOpen = true;
          CancelableClientCallStreamObserver<_SubscriptionItem> subscription;

          @Override
          public void close() {
            logger.info("Connection closed");
            isOpen = false;
            subscription.onError(new StatusRuntimeException(Status.UNAVAILABLE));
          }

          @Override
          public void open() {
            logger.info("Connection opened");
            isOpen = true;
          }

          @Override
          public void subscribe(
              _SubscriptionRequest subscriptionRequest,
              CancelableClientCallStreamObserver<_SubscriptionItem> subscription) {
            this.subscription = subscription;
            if (isOpen) {
              _SubscriptionItem heartbeat =
                  _SubscriptionItem.newBuilder()
                      .setHeartbeat(_Heartbeat.newBuilder().build())
                      .build();
              subscription.onNext(heartbeat);
            } else {
              subscription.onError(new StatusRuntimeException(Status.UNAVAILABLE));
            }
            waitingForSubscriptionAttempt.release();
          }
        };

    final SubscriptionRetryStrategy retryStrategy =
        new FixedDelaySubscriptionRetryStrategy(Duration.ofMillis(500));
    final SubscriptionWrapper subscriptionWrapper =
        new SubscriptionWrapper(
            cacheName,
            topicName,
            connection,
            callbacks,
            state,
            requestTimeoutSeconds,
            retryStrategy);
    final CompletableFuture<Void> subscribeWithRetryResult =
        subscriptionWrapper.subscribeWithRetry();
    subscribeWithRetryResult.join();

    waitingForSubscriptionAttempt.acquire();

    connection.close();

    assertTrue(gotConnectionLostCallback.get());
    assertFalse(gotConnectionRestoredCallback.get());

    connection.open();
    waitingForSubscriptionAttempt.acquire();

    assertTrue(gotConnectionRestoredCallback.get());

    subscriptionWrapper.close();
  }
}
