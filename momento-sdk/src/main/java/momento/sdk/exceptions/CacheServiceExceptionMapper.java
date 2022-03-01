package momento.sdk.exceptions;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.UnknownHostException;

public final class CacheServiceExceptionMapper {

  private static final String SDK_FAILED_TO_PROCESS_THE_REQUEST =
      "SDK Failed to process the request.";
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
  public static SdkException convert(Throwable e) {
    if (e instanceof SdkException) {
      throw (SdkException) e;
    }

    if (e instanceof io.grpc.StatusRuntimeException) {
      StatusRuntimeException grpcException = (StatusRuntimeException) e;
      switch (grpcException.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          // fall through
        case UNIMPLEMENTED:
          // fall through
        case OUT_OF_RANGE:
          // fall through
        case FAILED_PRECONDITION:
          return new BadRequestException(grpcException.getMessage());

        case CANCELLED:
          return new CancellationException(grpcException.getMessage());

        case DEADLINE_EXCEEDED:
          return new TimeoutException(grpcException.getMessage());

        case PERMISSION_DENIED:
          return new PermissionDeniedException(grpcException.getMessage());

        case UNAUTHENTICATED:
          return new AuthenticationException(grpcException.getMessage());

        case RESOURCE_EXHAUSTED:
          return new LimitExceededException(grpcException.getMessage());

        case NOT_FOUND:
          return new NotFoundException(grpcException.getMessage());

        case ALREADY_EXISTS:
          return new AlreadyExistsException(grpcException.getMessage());

        case UNKNOWN:
          // fall through
        case ABORTED:
          // fall through
        case INTERNAL:
          // fall through
        case UNAVAILABLE:
          // fall through
        case DATA_LOSS:
          // fall through
        default:
          return convertUnhandledExceptions(grpcException);
      }
    }

    return new ClientSdkException(SDK_FAILED_TO_PROCESS_THE_REQUEST, e);
  }

  public static SdkException convertUnhandledExceptions(StatusRuntimeException e) {
    if (isDnsUnreachable(e)) {
      return new InternalServerException(
          String.format(
              "Unable to reach request endpoint. Request failed with %s", e.getMessage()));
    }
    return new InternalServerException(INTERNAL_SERVER_ERROR_MESSAGE, e);
  }

  private static boolean isDnsUnreachable(StatusRuntimeException e) {
    return e.getStatus().getCode() == Status.Code.UNAVAILABLE
        && e.getCause() instanceof RuntimeException
        && e.getCause().getCause() instanceof UnknownHostException;
  }
}
