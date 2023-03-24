package momento.sdk.exceptions;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import momento.sdk.internal.MomentoGrpcErrorDetails;
import momento.sdk.internal.MomentoTransportErrorDetails;

public final class CacheServiceExceptionMapper {

  private static final String SDK_FAILED_TO_PROCESS_THE_REQUEST =
      "SDK Failed to process the request.";

  private CacheServiceExceptionMapper() {}

  /**
   * Common Handler for converting exceptions encountered by the SDK.
   *
   * <p>Any specialized exception handling should be performed before calling this
   *
   * @param e to convert
   */
  public static SdkException convert(Throwable e) {
    return convert(e, null);
  }

  /**
   * Common Handler for converting exceptions encountered by the SDK.
   *
   * <p>Any specialized exception handling should be performed before calling this
   *
   * @param e to convert
   * @param metadata metadata from the grpc request that caused the error
   */
  public static SdkException convert(Throwable e, Metadata metadata) {
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
          return new BadRequestException(grpcException, errorDetails);

        case CANCELLED:
          return new CancellationException(grpcException, errorDetails);

        case DEADLINE_EXCEEDED:
          return new TimeoutException(grpcException, errorDetails);

        case PERMISSION_DENIED:
          return new PermissionDeniedException(grpcException, errorDetails);

        case UNAUTHENTICATED:
          return new AuthenticationException(grpcException, errorDetails);

        case RESOURCE_EXHAUSTED:
          return new LimitExceededException(grpcException, errorDetails);

        case NOT_FOUND:
          return new NotFoundException(grpcException, errorDetails);

        case ALREADY_EXISTS:
          return new AlreadyExistsException(grpcException, errorDetails);

        case UNKNOWN:
          return new UnknownServiceException(grpcException, errorDetails);

        case UNAVAILABLE:
          return new ServerUnavailableException(grpcException, errorDetails);

        case ABORTED:
          // fall through
        case INTERNAL:
          // fall through
        case DATA_LOSS:
          // fall through
        default:
          return new InternalServerException(e, errorDetails);
      }
    }

    return new UnknownException(SDK_FAILED_TO_PROCESS_THE_REQUEST, e);
  }
}
