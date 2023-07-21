package momento.sdk.retry;

import com.google.common.base.Preconditions;
import java.util.Optional;

/**
 * A retry strategy that retries a failed gRPC call a fixed number of times, with no delay between
 * retry attempts.
 *
 * <p>The `FixedCountRetryStrategy` implements the {@link RetryStrategy} interface and provides a
 * fixed number of retry attempts without any delay between retries. It allows clients to specify
 * the maximum number of retry attempts.
 *
 * <p>When a gRPC call encounters an error, the `FixedCountRetryStrategy` retries the call a total
 * of maxAttempts times. The strategy does not introduce any delay between retries, effectively
 * retrying the call immediately for the fixed number of attempts specified.
 */
public class FixedCountRetryStrategy implements RetryStrategy {
  private final int maxAttempts;

  /**
   * Constructs a `FixedCountRetryStrategy` with the provided maximum number of retry attempts.
   *
   * @param maxAttempts The maximum number of retry attempts. After reaching this limit, no more
   *     retries will be performed, and the strategy will return an empty optional.
   * @throws IllegalArgumentException if maxAttempts is not greater than 0.
   */
  public FixedCountRetryStrategy(final int maxAttempts) {
    Preconditions.checkArgument(
        maxAttempts > 0, "Total number " + "of retry attempts should be greater than 0");
    this.maxAttempts = maxAttempts;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Long> getDelay(final int currentAttempt) {
    if (currentAttempt > maxAttempts) {
      return Optional.empty(); // No more retries after reaching the maximum attempts.
    }
    return Optional.of(0L); // Retry immediately with no delay for the fixed number of attempts.
  }
}
