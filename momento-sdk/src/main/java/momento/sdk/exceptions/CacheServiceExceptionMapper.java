package momento.sdk.exceptions;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import momento.sdk.internal.MomentoGrpcErrorDetails;
import momento.sdk.internal.MomentoTransportErrorDetails;

/**
 * Mapper from any exception that may occur during a cache client call to the appropriate {@link
 * SdkException}.
 */
public final class CacheServiceExceptionMapper {

  private static final String SDK_FAILED_TO_PROCESS_THE_REQUEST =
      "SDK Failed to process the request.";

  private CacheServiceExceptionMapper() {}

  /**
   * Common Handler for converting exceptions encountered by the SDK. Any specialized exception
   * handling should be performed before calling this.
   *
   * @param e The exception to convert.
   * @return The converted exception.
   */
  public static SdkException convert(Throwable e) {
    return convert(e, null);
  }

  /**
   * Common Handler for converting exceptions encountered by the SDK. Any specialized exception
   * handling should be performed before calling this.
   *
   * @param e The exception to convert.
   * @param metadata Metadata from the grpc request that caused the error.
   * @return The converted exception.
   */
  public static SdkException convert(Throwable e, Metadata metadata) {
    if (e instanceof SdkException) {
      return (SdkException) e;
    }

    if (e instanceof io.grpc.StatusRuntimeException) {
      final StatusRuntimeException grpcException = (StatusRuntimeException) e;
      final Status.Code statusCode = grpcException.getStatus().getCode();
      final Metadata trailers = grpcException.getTrailers();

      final MomentoTransportErrorDetails errorDetails =
          new MomentoTransportErrorDetails(
              new MomentoGrpcErrorDetails(statusCode, grpcException.getMessage(), metadata));

      String errorCause = trailers.get(Metadata.Key.of("err", Metadata.ASCII_STRING_MARSHALLER));
      if (errorCause == null) {
        errorCause = grpcException.getMessage();
      }

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
          if (errorCause.contains("element_not_found")) {
            return new StoreItemNotFoundException(grpcException, errorDetails);
          } else if (errorCause.contains("store_not_found")) {
            return new StoreNotFoundException(grpcException, errorDetails);
          } else {
            return new CacheNotFoundException(grpcException, errorDetails);
          }
        case ALREADY_EXISTS:
          if (errorCause.contains("Store with name")) {
            return new StoreAlreadyExistsException(grpcException, errorDetails);
          } else {
            return new CacheAlreadyExistsException(grpcException, errorDetails);
          }
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
