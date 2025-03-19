package momento.sdk.retry;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;

/**
 * A retry strategy that applies a fixed delay between consecutive retry attempts with an added
 * jitter to prevent retries from occurring at exact intervals.
 *
 * <p>The `FixedTimeoutRetryStrategy` implements the {@link RetryStrategy} interface and provides a
 * fixed delay interval between each retry attempt. This delay interval includes a jitter factor to
 * add randomness to the retry intervals, preventing retries from occurring at regular, predictable
 * intervals.
 *
 * <p>Additionally, the strategy enforces a `responseDataReceivedTimeoutMillis` value, which is the
 * number of milliseconds to wait for a response data to be received before retrying. If the
 * response data is not received within this time, the strategy will stop further retries
 * immediately.
 *
 * <p>The strategy also includes an eligibility check for retries, allowing the caller to specify
 * which gRPC statuses and methods should be eligible for retry. If the status or method is not
 * eligible for retry, the strategy will stop further retries immediately.
 *
 * <p>When a gRPC call encounters an error and is eligible for retry, the
 * `FixedTimeoutRetryStrategy` calculates the retry delay by adding a jitter factor to the base
 * delay interval. The jitter is a random variation applied to the retry delay, making retries
 * slightly more random.
 */
public class FixedTimeoutRetryStrategy implements RetryStrategy {
  private static final Random random = new Random();

  private final RetryEligibilityStrategy retryEligibilityStrategy;
  private final long retryDelayIntervalMillis;
  long responseDataReceivedTimeoutMillis;

  /**
   * Constructs a `FixedTimeoutRetryStrategy` with the provided parameters.
   *
   * @param eligibilityStrategy a strategy that determines if the gRPC status code and methods are
   *     eligible or safe to retry. If null, a default strategy is used.
   * @param retryDelayIntervalMillis The delay in milliseconds for each retry attempt. If null,
   *     default value of 100 milliseconds is used.
   * @param responseDataReceivedTimeoutMillis The number of milliseconds to wait for a response data
   *     to be received before retrying. If null, default value of 1000 milliseconds is used.
   */
  public FixedTimeoutRetryStrategy(
      RetryEligibilityStrategy eligibilityStrategy,
      Long retryDelayIntervalMillis,
      Long responseDataReceivedTimeoutMillis) {
    this.retryEligibilityStrategy =
        (eligibilityStrategy != null) ? eligibilityStrategy : new DefaultRetryEligibilityStrategy();
    this.retryDelayIntervalMillis =
        (retryDelayIntervalMillis != null) ? retryDelayIntervalMillis : 100;
    this.responseDataReceivedTimeoutMillis =
        (responseDataReceivedTimeoutMillis != null) ? responseDataReceivedTimeoutMillis : 1000;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Duration> determineWhenToRetry(
      final Status status, final MethodDescriptor methodDescriptor, final int currentAttempt) {

    if (!retryEligibilityStrategy.isEligibileForRetry(
        status, methodDescriptor.getFullMethodName())) {
      return Optional.empty();
    }

    return Optional.of(addJitter(retryDelayIntervalMillis));
  }

  private Duration addJitter(long whenToRetry) {
    return Duration.ofMillis((long) ((0.2 * random.nextDouble() + 0.9) * whenToRetry));
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Long> getResponseDataReceivedTimeoutMillis() {
    return Optional.of(responseDataReceivedTimeoutMillis);
  }
}
