package momento.sdk.exceptions;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.UnknownHostException;

public final class CacheServiceExceptionMapper {

  private static final String INTERNAL_SERVER_ERROR_MESSAGE =
      "Unexpected exception occurred while trying to fulfill the request.";

  private CacheServiceExceptionMapper() {}

  /**
   * Common Handler for converting exceptions encountered by the SDK.
   *
   * <p>Any specialized exception handling should be performed before calling this
   *
   * @param e
   */
  public static SdkException convert(Exception e) {
    if (e instanceof SdkException) {
      throw (SdkException) e;
    }

    if (e instanceof io.grpc.StatusRuntimeException) {
      StatusRuntimeException grpcException = (StatusRuntimeException) e;
      switch (grpcException.getStatus().getCode()) {
        case PERMISSION_DENIED:
          return new PermissionDeniedException(grpcException.getMessage());

        case NOT_FOUND:
          return new CacheNotFoundException(grpcException.getMessage());

        default:
          if (isDnsUnreachable(grpcException)) {
            return new ClientSdkException(
                String.format(
                    "Unable to reach request endpoint. Request failed with %s",
                    grpcException.getMessage()));
          }
          return new InternalServerException(INTERNAL_SERVER_ERROR_MESSAGE);
      }
    }

    return new ClientSdkException("SDK Failed to process the request", e);
  }

  private static boolean isDnsUnreachable(StatusRuntimeException e) {
    return e.getStatus().getCode() == Status.Code.UNAVAILABLE
        && e.getCause() instanceof RuntimeException
        && e.getCause().getCause() instanceof UnknownHostException;
  }
}
