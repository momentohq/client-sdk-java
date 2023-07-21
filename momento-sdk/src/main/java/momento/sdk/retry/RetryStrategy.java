package momento.sdk.retry;

import java.util.Optional;

/**
 * An interface representing a strategy for determining the delay between retry attempts for a
 * failed gRPC call.
 *
 * <p>Implementations of this interface allow clients to customize the delay between consecutive
 * retry attempts when a gRPC call fails. The {@link #getDelay(int)} method when provided to a
 * Configuration, is called by the RetryClientInterceptor to retrieve the delay for the next retry
 * attempt based on the current attempt number.
 *
 * <p>When a gRPC call encounters an error, the retry strategy can decide how long the client should
 * wait before attempting to retry the call. The delay can be constant, incrementally increasing, or
 * even follow a custom exponential or backoff pattern.
 *
 * <p>If the retry strategy returns an empty optional (Optional.empty()), it indicates that the
 * retry will not be performed based on the implemented contract. In such cases, the
 * RetryClientInterceptor will propagate the final result to the original listener after the last
 * failed attempt, effectively completing the call with the last status received without further
 * retries.
 *
 * <p>The value returned by {@link #getDelay(int)} should be a non-negative long value representing
 * the delay in milliseconds for the next retry attempt. If the returned value is 0 or a negative
 * number, the interceptor may interpret it as a request to retry immediately without any delay.
 *
 * <p>Note that the decision to retry and the specific delay calculation can be influenced by
 * various factors such as network conditions, server load, and the specific requirements of the
 * application.
 */
public interface RetryStrategy {

  /**
   * Retrieves the delay in milliseconds for the next retry attempt based on the current attempt
   * number.
   *
   * @param currentAttempt The current attempt number of the retry. Starts from 1 for the first
   *     retry attempt.
   * @return An {@link Optional} containing the delay in milliseconds for the next retry attempt, or
   *     an empty optional to indicate that no further retry attempts should be made.
   */
  Optional<Long> getDelay(int currentAttempt);
}
