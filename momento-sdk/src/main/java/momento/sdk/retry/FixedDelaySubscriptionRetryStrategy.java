package momento.sdk.retry;

import java.time.Duration;
import java.util.Optional;

/** A topic subscription retry strategy that uses a fixed delay between reconnection attempts. */
public class FixedDelaySubscriptionRetryStrategy implements SubscriptionRetryStrategy {

  private final SubscriptionRetryEligibilityStrategy eligibilityStrategy;
  private final Duration retryDelay;

  public FixedDelaySubscriptionRetryStrategy(
      SubscriptionRetryEligibilityStrategy eligibilityStrategy, Duration retryDelay) {
    this.eligibilityStrategy = eligibilityStrategy;
    this.retryDelay = retryDelay;
  }

  public FixedDelaySubscriptionRetryStrategy(Duration retryDelay) {
    this(new DefaultSubscriptionRetryEligibilityStrategy(), retryDelay);
  }

  @Override
  public Optional<Duration> determineWhenToRetry(Throwable error) {
    if (eligibilityStrategy.isEligibleForRetry(error)) {
      return Optional.of(retryDelay);
    }

    return Optional.empty();
  }
}
