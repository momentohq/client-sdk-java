package momento.sdk.retry;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSubscriptionRetryEligibilityStrategy
    implements SubscriptionRetryEligibilityStrategy {
  private static final Logger logger =
      LoggerFactory.getLogger(DefaultSubscriptionRetryEligibilityStrategy.class);

  private static final Set<Status.Code> NON_RETRYABLE_STATUS_CODES;

  static {
    final Set<Status.Code> nonRetryableStatusCodes = new HashSet<>();
    nonRetryableStatusCodes.add(Status.Code.PERMISSION_DENIED);
    nonRetryableStatusCodes.add(Status.Code.UNAUTHENTICATED);
    nonRetryableStatusCodes.add(Status.Code.CANCELLED);
    nonRetryableStatusCodes.add(Status.Code.NOT_FOUND);

    NON_RETRYABLE_STATUS_CODES = Collections.unmodifiableSet(nonRetryableStatusCodes);
  }

  @Override
  public boolean isEligibleForRetry(Throwable error) {
    if (error instanceof StatusRuntimeException) {
      final Status.Code statusCode = ((StatusRuntimeException) error).getStatus().getCode();
      if (NON_RETRYABLE_STATUS_CODES.contains(statusCode)) {
        logger.debug("Subscription error code {} is not retryable", statusCode);
        return false;
      }
      logger.debug("Subscription error code {} is retryable", statusCode);
      return true;
    } else {
      logger.debug("Unable to retry subscription", error);
      return false;
    }
  }
}
