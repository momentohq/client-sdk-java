package momento.sdk.retry;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;

import io.grpc.MethodDescriptor;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ExponentialBackoffRetryStrategyTest {

  @SuppressWarnings("rawtypes")
  @Mock
  private MethodDescriptor methodDescriptor;

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
  void whenFirstAttempt_shouldReturnInitialDelayWithJitter() {
    final ExponentialBackoffRetryStrategy strategy =
        new ExponentialBackoffRetryStrategy(100, 1000, eligibilityStrategy);

    for (int i = 0; i < 100; i++) {
      assertThat(strategy.determineWhenToRetry(status, methodDescriptor, 0))
          .hasValueSatisfying(delay -> assertThat(delay.toMillis()).isBetween(100L, 300L));
    }
  }

  @Test
  void whenSecondAttempt_shouldDoubleBaseDelayWithJitter() {
    final ExponentialBackoffRetryStrategy strategy =
        new ExponentialBackoffRetryStrategy(100, 1000, eligibilityStrategy);

    for (int i = 0; i < 100; i++) {
      assertThat(strategy.determineWhenToRetry(status, methodDescriptor, 1))
          .hasValueSatisfying(delay -> assertThat(delay.toMillis()).isBetween(200L, 600L));
    }
  }

  @Test
  void whenMaxBackoffReached_shouldNotExceedLimit() {
    final ExponentialBackoffRetryStrategy strategy =
        new ExponentialBackoffRetryStrategy(100, 500, eligibilityStrategy);

    assertThat(strategy.determineWhenToRetry(status, methodDescriptor, 100))
        .hasValueSatisfying(delay -> assertThat(delay.toMillis()).isBetween(500L, 1500L));
  }

  @Test
  void whenRetryNotEligible_shouldReturnEmpty() {
    final ExponentialBackoffRetryStrategy strategy =
        new ExponentialBackoffRetryStrategy(100, 1000, eligibilityStrategy);

    lenient()
        .when(eligibilityStrategy.isEligibileForRetry(eq(status), anyString()))
        .thenReturn(false);

    assertThat(strategy.determineWhenToRetry(status, methodDescriptor, 0)).isEmpty();
  }

  @Test
  void whenArithmeticOverflow_shouldUseMaxBackoff() {
    final ExponentialBackoffRetryStrategy strategy =
        new ExponentialBackoffRetryStrategy(1000, 10000, eligibilityStrategy);

    assertThat(strategy.determineWhenToRetry(status, methodDescriptor, Integer.MAX_VALUE))
        .hasValueSatisfying(delay -> assertThat(delay.toMillis()).isBetween(10000L, 30000L));
  }
}
