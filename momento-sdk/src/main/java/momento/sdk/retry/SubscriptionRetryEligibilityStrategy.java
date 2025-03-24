package momento.sdk.retry;

/**
 * An interface representing a strategy for determining whether a topic subscription is eligible for
 * reconnecting based on the error that disconnected it.
 *
 * <p>Implementations of this interface allow clients to customize the criteria for reconnecting a
 * failed subscription. {@link #isEligibleForRetry(Throwable)} is called by the
 * SubscriptionRetryStrategy to determine whether a specific disconnected subscription is eligible
 * for reconnecting.
 *
 * <p>A subscription may encounter different types of errors, such as network issues, or server-side
 * errors. For example, a simple implementation of this interface could check the status code of the
 * gRPC error and return true if the status code represents a transient error (e.g., unavailable),
 * or one that is unrecoverable (e.g., cache not found).
 */
public interface SubscriptionRetryEligibilityStrategy {

  /**
   * Determines whether a disconnected topic subscription is eligible for reconnection based on the
   * error that caused the disconnection.
   *
   * @param error The error encountered by the subscription when it disconnected.
   * @return {@code true} if the request is eligible for reconnection, {@code false} otherwise.
   */
  boolean isEligibleForRetry(Throwable error);
}
