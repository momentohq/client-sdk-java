package momento.sdk.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class FixedCountRetryStrategyTest {

  @Test
  void testGetDelay_SuccessfulRetry() {
    // Given a successful retry attempt
    int maxAttempts = 3;
    FixedCountRetryStrategy retryStrategy = new FixedCountRetryStrategy(maxAttempts);

    // When getting the delay for the first attempt
    Optional<Long> delay = retryStrategy.getDelay(1);

    // Then the delay should be 0, indicating an immediate retry
    assertTrue(delay.isPresent());
    assertEquals(0L, delay.get());
  }

  @Test
  void testGetDelay_ExceededMaxAttempts() {
    // Given that retry attempts have exceeded the maximum allowed attempts
    int maxAttempts = 3;
    FixedCountRetryStrategy retryStrategy = new FixedCountRetryStrategy(maxAttempts);

    // When getting the delay for the fourth attempt
    Optional<Long> delay = retryStrategy.getDelay(4);

    // Then the delay should be empty, indicating no more retries
    assertFalse(delay.isPresent());
  }
}
