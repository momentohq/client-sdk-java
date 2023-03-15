package momento.sdk.exceptions;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import momento.sdk.internal.MomentoGrpcErrorDetails;
import momento.sdk.internal.MomentoTransportErrorDetails;

public final class CacheServiceExceptionMapper {

  private static final String SDK_FAILED_TO_PROCESS_THE_REQUEST =
      "SDK Failed to process the request.";
  private static final String INTERNAL_SERVER_ERROR_MESSAGE =
      "Unexpected exception occurred while trying to fulfill the request.";

  private CacheServiceExceptionMapper() {}

  public static SdkException convert(Throwable e) {
    return convert(e, Collections.emptyMap());
  }

  /**
   * Common Handler for converting exceptions encountered by the SDK.
   *
   * <p>Any specialized exception handling should be performed before calling this
   *
   * @param e to convert
   */
  public static SdkException convert(Throwable e, Map<String, String> metadata) {
    if (e instanceof SdkException) {
      return (SdkException) e;
    }

    if (e instanceof io.grpc.StatusRuntimeException) {
      final StatusRuntimeException grpcException = (StatusRuntimeException) e;
      final Status.Code statusCode = grpcException.getStatus().getCode();

      final MomentoTransportErrorDetails errorDetails =
          new MomentoTransportErrorDetails(
              new MomentoGrpcErrorDetails(statusCode, grpcException.getMessage(), metadata));

      switch (statusCode) {
        case INVALID_ARGUMENT:
          // fall through
        case UNIMPLEMENTED:
          // fall through
        case OUT_OF_RANGE:
          // fall through
        case FAILED_PRECONDITION:
          return new BadRequestException(grpcException.getMessage(), errorDetails);

        case CANCELLED:
          return new CancellationException(grpcException.getMessage(), errorDetails);

        case DEADLINE_EXCEEDED:
          return new TimeoutException(grpcException.getMessage(), errorDetails);

        case PERMISSION_DENIED:
          return new PermissionDeniedException(grpcException.getMessage(), errorDetails);

        case UNAUTHENTICATED:
          return new AuthenticationException(grpcException.getMessage(), errorDetails);

        case RESOURCE_EXHAUSTED:
          return new LimitExceededException(grpcException.getMessage(), errorDetails);

        case NOT_FOUND:
          return new NotFoundException(grpcException.getMessage(), errorDetails);

        case ALREADY_EXISTS:
          return new AlreadyExistsException(grpcException.getMessage(), errorDetails);

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
          return convertUnhandledExceptions(grpcException, errorDetails);
      }
    }

    return new ClientSdkException(SDK_FAILED_TO_PROCESS_THE_REQUEST, e);
  }

  public static SdkException convertUnhandledExceptions(
      StatusRuntimeException e, MomentoTransportErrorDetails errorDetails) {
    if (isDnsUnreachable(e)) {
      return new InternalServerException(
          String.format("Unable to reach request endpoint. Request failed with %s", e.getMessage()),
          errorDetails);
    }
    return new InternalServerException(INTERNAL_SERVER_ERROR_MESSAGE, e, errorDetails);
  }

  private static boolean isDnsUnreachable(StatusRuntimeException e) {
    return e.getStatus().getCode() == Status.Code.UNAVAILABLE
        && e.getCause() instanceof RuntimeException
        && e.getCause().getCause() instanceof UnknownHostException;
  }
}
