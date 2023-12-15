package momento.sdk;

import grpc.cache_client.pubsub._Heartbeat;
import grpc.cache_client.pubsub._SubscriptionItem;
import grpc.cache_client.pubsub._SubscriptionRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import momento.sdk.internal.SubscriptionState;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SubscriptionWrapperTest {
  private final Logger logger = LoggerFactory.getLogger(SubscriptionWrapperTest.class);

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testConnectionLostAndRestored() throws InterruptedException {
    SubscriptionState state = new SubscriptionState();
    TopicSubscribeResponse.Subscription subscription =
        new TopicSubscribeResponse.Subscription(state);

    AtomicBoolean gotConnectionLostCallback = new AtomicBoolean(false);
    AtomicBoolean gotConnectionRestoredCallback = new AtomicBoolean(false);

    Semaphore waitingForSubscriptionAttempt = new Semaphore(0);

    SendSubscribeOptions options =
        new SendSubscribeOptions(
            "cache",
            "topic",
            (message) -> {},
            () -> {},
            (err) -> {},
            () -> {
              logger.info("Got to our connection lost callback!");
              gotConnectionLostCallback.set(true);
            },
            () -> {
              logger.info("Got to our connection restored callback!");
              gotConnectionRestoredCallback.set(true);
            },
            state,
            subscription);

    IScsTopicConnection connection =
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

    SubscriptionWrapper subscriptionWrapper = new SubscriptionWrapper(connection, options);
    CompletableFuture<Void> subscribeWithRetryResult = subscriptionWrapper.subscribeWithRetry();
    subscribeWithRetryResult.join();

    waitingForSubscriptionAttempt.acquire();

    connection.close();

    assertTrue(gotConnectionLostCallback.get());
    assertFalse(gotConnectionRestoredCallback.get());

    connection.open();
    waitingForSubscriptionAttempt.acquire();

    assertTrue(gotConnectionRestoredCallback.get());
  }
}
