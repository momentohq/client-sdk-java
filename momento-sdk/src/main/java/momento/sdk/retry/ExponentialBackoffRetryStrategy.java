package momento.sdk.retry;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Optional;

/**
 * A retry strategy that retries a failed gRPC call an unlimited number of times until the gRPC
 * deadline, with a jittered, exponentially increasing delay between attempts. The delay of the
 * first attempt is randomly selected within a range between the initial delay, and the initial
 * delay * 3. The maximum possible delay is randomly selected within a range between the max backoff
 * / 2, and the max backoff * 3.
 *
 * <p>The `FixedCountRetryStrategy` implements the {@link RetryStrategy} interface and provides a
 * fixed number of retry attempts without any delay between retries. It allows clients to specify
 * the maximum number of retry attempts.
 */
public class ExponentialBackoffRetryStrategy implements RetryStrategy {
  private static final int GROWTH_FACTOR = 2;

  private final long initialDelayMillis;
  private final long maxBackoffMillis;
  private final RetryEligibilityStrategy retryEligibilityStrategy;

  /**
   * Constructs an `ExponentialBackoffRetryStrategy` with an initial delay of 1 millisecond and a
   * max backoff of 8 milliseconds. Including jitter, the first retry delay will be between 1 and 3
   * ms, and the maximum retry delay will be between 8 and 12 ms.
   */
  public ExponentialBackoffRetryStrategy() {
    this(1, 8, new DefaultRetryEligibilityStrategy());
  }

  /**
   * Constructs an `ExponentialBackoffRetryStrategy` with the given initial delay and max backoff
   * times.
   *
   * @param initialDelayMillis The lower bound for the first retry delay. The initial delay range is
   *     between this value, and this value * 3.
   * @param maxBackoffMillis The upper bound for the retry delay growth. The largest delay range is
   *     between this value / 2, and this value * 3.
   */
  public ExponentialBackoffRetryStrategy(int initialDelayMillis, int maxBackoffMillis) {
    this(initialDelayMillis, maxBackoffMillis, new DefaultRetryEligibilityStrategy());
  }

  /**
   * Constructs an `ExponentialBackoffRetryStrategy` with the given initial delay and max backoff
   * times, and the given eligibility strategy.
   *
   * @param initialDelayMillis The lower bound for the first retry delay. The initial delay range is
   *     between this value, and this value * 3.
   * @param maxBackoffMillis The upper bound for the retry delay growth. The largest delay range is
   *     between this value / 2, and this value * 3.
   * @param retryEligibilityStrategy Determines if a call is eligible to be retried based on the
   *     method being called and the gRPC status code of the previous failure.
   */
  public ExponentialBackoffRetryStrategy(
      int initialDelayMillis,
      int maxBackoffMillis,
      RetryEligibilityStrategy retryEligibilityStrategy) {
    this.initialDelayMillis = initialDelayMillis;
    this.maxBackoffMillis = maxBackoffMillis;
    this.retryEligibilityStrategy = retryEligibilityStrategy;
  }

  @Override
  public Optional<Duration> determineWhenToRetry(
      Status status,
      @SuppressWarnings("rawtypes") MethodDescriptor methodDescriptor,
      int currentAttempt) {
    if (!retryEligibilityStrategy.isEligibileForRetry(
        status, methodDescriptor.getFullMethodName())) {
      return Optional.empty();
    }

    final long baseDelay = computeBaseDelay(currentAttempt);
    final long previousBaseDelay = computePreviousBaseDelay(baseDelay);
    final long maxDelay = previousBaseDelay * 3;
    final long jitteredDelay = randomInRange(baseDelay, maxDelay);

    return Optional.of(Duration.ofMillis(jitteredDelay));
  }

  private long computeBaseDelay(int attemptNumber) {
    if (attemptNumber <= 0) {
      return this.initialDelayMillis;
    }

    try {
      final long multiplier = (long) Math.pow(GROWTH_FACTOR, attemptNumber);
      final long baseDelay = Math.multiplyExact(this.initialDelayMillis, multiplier);
      return Math.min(baseDelay, maxBackoffMillis);
    } catch (ArithmeticException e) {
      return maxBackoffMillis;
    }
  }

  private long computePreviousBaseDelay(long currentBaseDelay) {
    if (currentBaseDelay == initialDelayMillis) {
      return initialDelayMillis;
    }

    return currentBaseDelay / GROWTH_FACTOR;
  }

  private long randomInRange(long min, long max) {
    if (min >= max) {
      return min;
    }
    return min + (long) (Math.random() * (max - min));
  }
}
