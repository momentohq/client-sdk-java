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
  }

  private static final Set<String> RETRYABLE_GRPC_FULL_METHOD_NAMES =
      new HashSet<String>() {
        {
          add("cache_client.Scs/Get");
          add("cache_client.Scs/GetBatch");
          add("cache_client.Scs/Set");
          add("cache_client.Scs/SetBatch");
          // Not retryable: "cache_client.Scs/SetIf"
          // SetIfNotExists is deprecated
          // Not retryable: "cache_client.Scs/SetIfNotExists"
          add("cache_client.Scs/Delete");
          add("cache_client.Scs/KeysExist");
          // Not retryable: "cache_client.Scs/Increment"
          // Not retryable: "cache_client.Scs/UpdateTtl"
          add("cache_client.Scs/ItemGetTtl");
          add("cache_client.Scs/ItemGetType");

          add("cache_client.Scs/DictionaryGet");
          add("cache_client.Scs/DictionaryFetch");
          add("cache_client.Scs/DictionarySet");
          // Not retryable: "cache_client.Scs/DictionaryIncrement"
          add("cache_client.Scs/DictionaryDelete");
          add("cache_client.Scs/DictionaryLength");

          add("cache_client.Scs/SetFetch");
          add("cache_client.Scs/SetSample");
          add("cache_client.Scs/SetUnion");
          add("cache_client.Scs/SetDifference");
          add("cache_client.Scs/SetContains");
          add("cache_client.Scs/SetLength");
          // Not retryable: "cache_client.Scs/SetPop"

          // Not retryable: "cache_client.Scs/ListPushFront"
          // Not retryable: "cache_client.Scs/ListPushBack"
          // Not retryable: "cache_client.Scs/ListPopFront"
          // Not retryable: "cache_client.Scs/ListPopBack"
          // Not used: "cache_client.Scs/ListErase"
          add("cache_client.Scs/ListRemove");
          add("cache_client.Scs/ListFetch");
          add("cache_client.Scs/ListLength");
          // Not retryable: "cache_client.Scs/ListConcatenateFront"
          // Not retryable: "cache_client.Scs/ListConcatenateBack"
          // Not retryable: "cache_client.Scs/ListRetain"

          add("cache_client.Scs/SortedSetPut");
          add("cache_client.Scs/SortedSetFetch");
          add("cache_client.Scs/SortedSetGetScore");
          add("cache_client.Scs/SortedSetRemove");
          // Not retryable: "cache_client.Scs/SortedSetIncrement"
          add("cache_client.Scs/SortedSetGetRank");
          add("cache_client.Scs/SortedSetLength");
          add("cache_client.Scs/SortedSetLengthByScore");

          add("cache_client.pubsub.Pubsub/Subscribe");
        }
      };

  /** {@inheritDoc} */
  public boolean isEligibileForRetry(final Status status, final String methodName) {
    return RETRYABLE_GRPC_FULL_METHOD_NAMES.contains(methodName)
        && RETRYABLE_STATUS_CODES.contains(status.getCode());
  }
}
