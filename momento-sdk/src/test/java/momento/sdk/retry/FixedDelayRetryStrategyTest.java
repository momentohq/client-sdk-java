package momento.sdk.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FixedDelayRetryStrategyTest {

  @Mock private MethodDescriptor methodDescriptor;
  @Mock private Status status;

  @Mock private RetryEligibilityStrategy eligibilityStrategy;

  @BeforeEach
  public void setup() {
    lenient().when(methodDescriptor.getFullMethodName()).thenReturn("methodName");
    lenient()
        .when(eligibilityStrategy.isEligibileForRetry(eq(status), anyString()))
        .thenReturn(true);
  }

  @Test
  void testGetDelay_SuccessfulRetry() {
    // Given a successful retry attempt
    int maxAttempts = 3;
    long delayMillis = 1000L;
    long maxDelayMillis = 3000L;
    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    // When getting the delay for the first attempt
    Optional<Duration> delay = retryStrategy.determineWhenToRetry(status, methodDescriptor, 1);

    // Then the delay should be equal to the fixed delay
    assertTrue(delay.isPresent());
    assertEquals(delayMillis, delay.get().toMillis());
  }

  @Test
  void testGetDelay_ExceededMaxAttempts() {
    // Given that retry attempts have exceeded the maximum allowed attempts
    int maxAttempts = 3;
    long delayMillis = 1000L;
    long maxDelayMillis = 3000L;
    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    // When getting the delay for the fourth attempt
    Optional<Duration> delay = retryStrategy.determineWhenToRetry(status, methodDescriptor, 4);

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
        new FixedDelayRetryStrategy(maxAttempts, delayMillis, maxDelayMillis, eligibilityStrategy);

    // When getting the delay for the tenth attempt
    Optional<Duration> delay = retryStrategy.determineWhenToRetry(status, methodDescriptor, 6);

    // Then the delay should be empty, indicating no more retries
    assertFalse(delay.isPresent());
  }

  @Test
  void testGetDelay_IneligibleToRetry() {
    when(eligibilityStrategy.isEligibileForRetry(eq(status), anyString())).thenReturn(false);

    int maxAttempts = 3;
    FixedDelayRetryStrategy retryStrategy =
        new FixedDelayRetryStrategy(maxAttempts, 100, 500, eligibilityStrategy);

    // valid attempt but not eligible
    Optional<Duration> delay = retryStrategy.determineWhenToRetry(status, methodDescriptor, 1);

    // Then the delay should be empty, indicating no more retries
    assertFalse(delay.isPresent());
  }
}
