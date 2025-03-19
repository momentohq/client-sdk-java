package momento.sdk.retry;

import java.time.Duration;
import java.util.Optional;

/**
 * An interface representing a strategy for determining the delay between reconnection attempts for
 * a disconnected topic subscription.
 *
 * <p>Implementations of this interface allow clients to customize the delay between consecutive
 * reconnection attempts when a subscription fails. The {@link #determineWhenToRetry(Throwable)}
 * method is called by the subscription to retrieve the delay for the next reconnection attempt
 * based on the error encountered when the connection attempt failed.
 *
 * <p>If the retry strategy returns an empty optional (Optional.empty()), it indicates that the
 * reconnection will not be performed based on the implemented contract. In such cases, the
 * subscription will call {@link momento.sdk.ISubscriptionCallbacks#onError(Throwable)} and no
 * further reconnection attempts will be made.
 */
public interface SubscriptionRetryStrategy {

  /**
   * Returns the amount of time before the next subscription reconnection attempt.
   *
   * @param error The error encountered by the subscription when it disconnected.
   * @return A duration to wait before reconnecting, or empty if no reconnection attempt should be
   *     made.
   */
  Optional<Duration> determineWhenToRetry(Throwable error);
}
