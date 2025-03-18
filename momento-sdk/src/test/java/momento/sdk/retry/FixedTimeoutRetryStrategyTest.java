package momento.sdk.retry;

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
public class FixedTimeoutRetryStrategyTest {

  @Mock private MethodDescriptor methodDescriptor;
  @Mock private Status status;

  @Mock private RetryEligibilityStrategy eligibilityStrategy;

  @BeforeEach
  public void setUp() {
    lenient().when(methodDescriptor.getFullMethodName()).thenReturn("methodName");
    lenient()
        .when(eligibilityStrategy.isEligibileForRetry(eq(status), anyString()))
        .thenReturn(true);
  }

  @Test
  void testGetTimeout_SuccessfulRetry() {
    long retryIntervalDelayMillis = 100;
    long responseDataReceivedTimeoutMillis = 200;
    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryIntervalDelayMillis, responseDataReceivedTimeoutMillis);
    Optional<Duration> delay = retryStrategy.determineWhenToRetry(status, methodDescriptor, 1);
    assertTrue(delay.isPresent());
    assertTrue(delay.get().toMillis() <= responseDataReceivedTimeoutMillis);
  }

  @Test
  void testGetTimeout_IneligibleToRetry() {
    when(eligibilityStrategy.isEligibileForRetry(eq(status), anyString())).thenReturn(false);
    long retryIntervalDelayMillis = 100;
    long responseDataReceivedTimeoutMillis = 200;
    FixedTimeoutRetryStrategy retryStrategy =
        new FixedTimeoutRetryStrategy(
            eligibilityStrategy, retryIntervalDelayMillis, responseDataReceivedTimeoutMillis);
    Optional<Duration> delay = retryStrategy.determineWhenToRetry(status, methodDescriptor, 1);
    assertFalse(delay.isPresent());
  }
}
