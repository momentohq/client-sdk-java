package momento.sdk.retry;

import io.grpc.Status;
import java.util.HashSet;
import java.util.Set;

/**
 * DefaultRetryEligibilityStrategy is an implementation of the RetryEligibilityStrategy interface
 * that determines whether a gRPC call is eligible for retry based on its method name and status
 * code.
 */
public class DefaultRetryEligibilityStrategy implements RetryEligibilityStrategy {

  private static final Set<Status.Code> RETRYABLE_STATUS_CODES = new HashSet<>();

  static {
    RETRYABLE_STATUS_CODES.add(Status.Code.UNAVAILABLE);
    RETRYABLE_STATUS_CODES.add(Status.Code.INTERNAL);
    RETRYABLE_STATUS_CODES.add(Status.Code.NOT_FOUND);
  }

  private static final Set<String> RETRYABLE_GRPC_FULL_METHOD_NAMES =
      new HashSet<String>() {
        {
          add("cache_client.Scs/Get");
          add("cache_client.Scs/Set");
          add("cache_client.Scs/Delete");
          // not idempotent "/cache_client.Scs/Increment"
          add("cache_client.Scs/DictionarySet");
          // not idempotent: "/cache_client.Scs/DictionaryIncrement",
          add("cache_client.Scs/DictionaryGet");
          add("cache_client.Scs/DictionaryFetch");
          add("cache_client.Scs/DictionaryDelete");
          add("cache_client.Scs/SetUnion");
          add("cache_client.Scs/SetDifference");
          add("cache_client.Scs/SetFetch");
          // not idempotent: "/cache_client.Scs/SetIfNotExists"
          // not idempotent: "/cache_client.Scs/ListPushFront",
          // not idempotent: "/cache_client.Scs/ListPushBack",
          // not idempotent: "/cache_client.Scs/ListPopFront",
          // not idempotent: "/cache_client.Scs/ListPopBack"
          add("cache_client.Scs/ListFetch");
          // Warning: in the future, this may not be idempotent
          // Currently it supports removing all occurrences of a value.
          // In the future, we may also add "the first/last N occurrences of a value".
          // In the latter case it is not idempotent.
          add("cache_client.Scs/ListRemove");
          add("cache_client.Scs/ListLength");
          // not idempotent: "/cache_client.Scs/ListConcatenateFront",
          // not idempotent: "/cache_client.Scs/ListConcatenateBack"
        }
      };

  /** {@inheritDoc} */
  public boolean isEligibileForRetry(final Status status, final String methodName) {
    return RETRYABLE_GRPC_FULL_METHOD_NAMES.contains(methodName)
        && RETRYABLE_STATUS_CODES.contains(status.getCode());
  }
}
