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
class FixedCountRetryStrategyTest {

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
    FixedCountRetryStrategy retryStrategy =
        new FixedCountRetryStrategy(maxAttempts, eligibilityStrategy);

    // When getting the delay for the first attempt
    Optional<Duration> delay = retryStrategy.determineWhenToRetry(status, methodDescriptor, 1);

    // Then the delay should be 0, indicating an immediate retry
    assertTrue(delay.isPresent());
    assertEquals(0L, delay.get().toMillis());
  }

  @Test
  void testGetDelay_ExceededMaxAttempts() {
    // Given that retry attempts have exceeded the maximum allowed attempts
    int maxAttempts = 3;
    FixedCountRetryStrategy retryStrategy =
        new FixedCountRetryStrategy(maxAttempts, eligibilityStrategy);

    // When getting the delay for the fourth attempt
    Optional<Duration> delay = retryStrategy.determineWhenToRetry(status, methodDescriptor, 4);

    // Then the delay should be empty, indicating no more retries
    assertFalse(delay.isPresent());
  }

  @Test
  void testGetDelay_IneligibleToRetry() {
    when(eligibilityStrategy.isEligibileForRetry(eq(status), anyString())).thenReturn(false);

    int maxAttempts = 3;
    FixedCountRetryStrategy retryStrategy =
        new FixedCountRetryStrategy(maxAttempts, eligibilityStrategy);

    // valid attempt but not eligible
    Optional<Duration> delay = retryStrategy.determineWhenToRetry(status, methodDescriptor, 1);

    // Then the delay should be empty, indicating no more retries
    assertFalse(delay.isPresent());
  }
}
