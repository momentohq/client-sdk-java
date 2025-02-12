package momento.sdk.retry;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Optional;

public class ExponentialBackoffRetryStrategy implements RetryStrategy {
  private static final int GROWTH_FACTOR = 2;

  private final long initialDelayMillis;
  private final long maxBackoffMillis;
  private final RetryEligibilityStrategy retryEligibilityStrategy;

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
    return currentBaseDelay / GROWTH_FACTOR;
  }

  private long randomInRange(long min, long max) {
    if (min >= max) {
      return min;
    }
    return min + (long) (Math.random() * (max - min));
  }
}
