package momento.sdk.retry;

import io.grpc.Status;

/**
 * An interface representing a strategy for determining whether a gRPC request is eligible for
 * retrying based on the status and method name.
 *
 * <p>Implementations of this interface allow clients to customize the criteria for retrying a
 * failed gRPC call. The {@link #isEligibileForRetry(Status, String)} method is called by the
 * RetryClientInterceptor to determine whether a specific gRPC request is eligible for retrying or
 * not.
 *
 * <p>A gRPC call may encounter different types of errors, such as network issues, server-side
 * errors, or client-side errors. The retry eligibility strategy can evaluate the gRPC status and
 * the associated method name to make an informed decision on whether to retry the call or not.
 *
 * <p>For example, a simple implementation of this interface could check the status code of the gRPC
 * response and return true if the status code represents a transient error (e.g., 503 Service
 * Unavailable). More advanced implementations could take into account other factors such as the
 * error message, specific error codes, or custom conditions specific to the application.
 */
public interface RetryEligibilityStrategy {
  /**
   * Determines whether a gRPC request is eligible for retrying based on the provided status and
   * method name.
   *
   * @param status The gRPC status returned by the server after the initial request.
   * @param methodName The full method name of the gRPC request (e.g.,
   *     "com.example.FooService/BarMethod").
   * @return {@code true} if the request is eligible for retry, {@code false} otherwise.
   */
  boolean isEligibileForRetry(Status status, String methodName);
}
