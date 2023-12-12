package momento.sdk.responses.topic;

import java.util.function.Supplier;

import io.grpc.StatusRuntimeException;
import momento.sdk.exceptions.SdkException;

/** Represents the response for a topic subscribe operation. */
public interface TopicSubscribeResponse {

  /** Represents a successful topic subscribe operation. */
  class Subscription implements TopicSubscribeResponse {
    private SubscriptionState subscriptionState;

    /**
     * Constructs a Subscription instance with the provided subscription state.
     *
     * @param subscriptionState The subscription state.
     */
    public Subscription(SubscriptionState subscriptionState) {
      super();
      this.subscriptionState = subscriptionState;
    }

    /** Unsubscribes from the topic. */
    public void unsubscribe() {
      this.subscriptionState.unsubscribe();
    }

    public void hackyOnError(StatusRuntimeException deadlineExceeded) {
      this.subscriptionState.hackySubscriptionWrapper.subscription.onError(deadlineExceeded);
    }
  }

  /**
   * Represents a failed topic subscribe operation. The response itself is an exception, so it can
   * be directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements TopicSubscribeResponse {

    /**
     * Constructs a topic subscribe error with a cause.
     *
     * @param cause The cause of the error.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }

  /**
   * Returns the Subscription if the response is a Subscription, otherwise returns the result of
   * invoking the specified Supplier.
   *
   * @param supplier The Supplier to be invoked if the response is not a Subscription.
   * @return The Subscription if the response is a Subscription, otherwise the result of invoking
   *     the specified Supplier.
   */
  default Subscription orElseGet(Supplier<? extends Subscription> supplier) {
    return this instanceof Subscription ? (Subscription) this : supplier.get();
  }

  /**
   * Returns the Subscription if the response is a Subscription, otherwise throws an exception
   * produced by the specified Supplier.
   *
   * @param <X> The type of the exception to be thrown.
   * @param exceptionSupplier The Supplier that produces the exception to be thrown.
   * @return The Subscription if the response is a Subscription.
   * @throws X if the response is not a Subscription.
   */
  default <X extends Throwable> Subscription orElseThrow(Supplier<? extends X> exceptionSupplier)
      throws X {
    if (this instanceof Subscription) {
      return (Subscription) this;
    } else {
      throw exceptionSupplier.get();
    }
  }
}
