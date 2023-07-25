package momento.sdk.retry;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * A retry strategy that uses a fixed delay between consecutive retry attempts for a failed gRPC
 * call.
 *
 * <p>The `FixedDelayRetryStrategy` implements the {@link RetryStrategy} interface and provides a
 * fixed delay for each retry attempt. It allows clients to specify the maximum number of retry
 * attempts, the initial delay, and the maximum delay between attempts.
 *
 * <p>When a gRPC call encounters an error, the `FixedDelayRetryStrategy` calculates the delay for
 * each retry attempt based on the provided delayMillis. The delay for each retry attempt is
 * cumulative, where the nth attempt has a delay of n * delayMillis. If the cumulative delay exceeds
 * the specified maxDelayMillis or the number of attempts exceeds maxAttempts, the retry strategy
 * stops further retries and returns an empty optional to indicate that no more retry attempts
 * should be made.
 */
public class FixedDelayRetryStrategy implements RetryStrategy {
  private final int maxAttempts;
  private final long delayMillis;
  private final long maxDelayMillis;

  private final RetryEligibilityStrategy retryEligibilityStrategy;

  /**
   * Constructs a `FixedDelayRetryStrategy` with the provided parameters.
   *
   * @param maxAttempts The maximum number of retry attempts. After reaching this limit, no more
   *     retries will be performed, and the strategy will return an empty optional.
   * @param delayMillis The delay in milliseconds for each retry attempt.
   * @param maxDelayMillis The maximum cumulative delay in milliseconds that is allowed for all
   *     retry attempts combined. If the cumulative delay exceeds this value, no more retries will
   *     be performed, and the strategy will return an empty optional.
   * @param retryEligibilityStrategy a strategy that determines if the gRPC status code and methods
   *     are eligible or safe to retry.
   * @throws IllegalArgumentException if delayMillis is greater than maxDelayMillis.
   */
  public FixedDelayRetryStrategy(
      int maxAttempts,
      long delayMillis,
      long maxDelayMillis,
      RetryEligibilityStrategy retryEligibilityStrategy) {
    assert delayMillis <= maxDelayMillis : "Delay amount should be " + "less than or equal " +
            "to the maximum delay";
    Objects.requireNonNull(
        retryEligibilityStrategy, "Retry eligibility strategy should not be null");
    this.maxAttempts = maxAttempts;
    this.delayMillis = delayMillis;
    this.maxDelayMillis = maxDelayMillis;
    this.retryEligibilityStrategy = retryEligibilityStrategy;
  }

  /** {@inheritDoc} * */
  public FixedDelayRetryStrategy(int maxAttempts, long delayMillis, long maxDelayMillis) {
    assert delayMillis <= maxDelayMillis : "Delay amount should be less than or equal " +
            "to the maximum delay";
    this.maxAttempts = maxAttempts;
    this.delayMillis = delayMillis;
    this.maxDelayMillis = maxDelayMillis;
    this.retryEligibilityStrategy = new DefaultRetryEligibilityStrategy();
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Duration> determineWhenToRetry(
      final Status status, final MethodDescriptor methodDescriptor, final int currentAttempt) {

    if (!retryEligibilityStrategy.isEligibileForRetry(
        status, methodDescriptor.getFullMethodName())) {
      return Optional.empty();
    }

    if (currentAttempt > maxAttempts) {
      return Optional.empty(); // Exceeded the maximum number of retry attempts.
    }

    long cumulativeDelay = delayMillis * currentAttempt;
    // If the cumulative delay exceeds the maximum allowed delay, stop further retries.
    if (cumulativeDelay > maxDelayMillis) {
      return Optional.empty();
    }

    return Optional.of(
        Duration.ofMillis(delayMillis)); // Return the fixed delay for the current attempt.
  }
}
