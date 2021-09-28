package momento.sdk.exceptions;

import io.grpc.StatusRuntimeException;

public final class CacheServiceExceptionMapper {

  private CacheServiceExceptionMapper() {}

  /**
   * Common Handler for converting exceptions encountered by the SDK.
   *
   * <p>Any specialized exception handling should be performed before calling this
   *
   * @param e
   */
  public static void convertAndThrow(Exception e) throws SdkException {
    if (e instanceof SdkException) {
      throw (SdkException) e;
    }

    if (e instanceof io.grpc.StatusRuntimeException) {
      StatusRuntimeException grpcException = (StatusRuntimeException) e;
      switch (grpcException.getStatus().getCode()) {
        case PERMISSION_DENIED:
          throw new PermissionDeniedException(grpcException.getMessage());

        default:
          throw new InternalServerException(
              "Unexpected exception occurred while trying to fulfill the request.");
      }
    }

    throw new ClientSdkException("SDK Failed to process the request", e);
  }
}
