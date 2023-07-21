package momento.sdk.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class FixedDelayRetryStrategyTest {

  @Test
  void testGetDelay_SuccessfulRetry() {
    // Given a successful retry attempt
    int maxAttempts = 3;
    long delayMillis = 1000L;
    long maxDelayMillis = 3000L;
    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis);

    // When getting the delay for the first attempt
    Optional<Long> delay = retryStrategy.getDelay(1);

    // Then the delay should be equal to the fixed delay
    assertTrue(delay.isPresent());
    assertEquals(delayMillis, delay.get());
  }

  @Test
  void testGetDelay_ExceededMaxAttempts() {
    // Given that retry attempts have exceeded the maximum allowed attempts
    int maxAttempts = 3;
    long delayMillis = 1000L;
    long maxDelayMillis = 3000L;
    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis);

    // When getting the delay for the fourth attempt
    Optional<Long> delay = retryStrategy.getDelay(4);

    // Then the delay should be empty, indicating no more retries
    assertFalse(delay.isPresent());
  }

  @Test
  void testGetDelay_ExceededMaxDelay() {
    // Given that the cumulative delay has exceeded the maximum delay
    int maxAttempts = 10;
    long delayMillis = 100L;
    long maxDelayMillis = 500L;
    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis);

    // When getting the delay for the tenth attempt
    Optional<Long> delay = retryStrategy.getDelay(10);

    // Then the delay should be empty, indicating no more retries
    assertFalse(delay.isPresent());
  }
}
