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
  public static SdkException convert(Exception e) {
    if (e instanceof SdkException) {
      throw (SdkException) e;
    }

    if (e instanceof io.grpc.StatusRuntimeException) {
      StatusRuntimeException grpcException = (StatusRuntimeException) e;
      switch (grpcException.getStatus().getCode()) {
        case PERMISSION_DENIED:
          return new PermissionDeniedException(grpcException.getMessage());

        default:
          return new InternalServerException(
              "Unexpected exception occurred while trying to fulfill the request.");
      }
    }

    return new ClientSdkException("SDK Failed to process the request", e);
  }
}
