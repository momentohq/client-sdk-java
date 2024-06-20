package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Requested resource or the resource on which an operation was requested doesn't exist. */
public class CacheNotFoundException extends MomentoServiceException {

  private static final String MESSAGE =
      "A cache with the specified name does not exist. To resolve this error, "
          + "make sure you have created the cache before attempting to use it.";

  /**
   * Constructs a NotFoundException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public CacheNotFoundException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(
        MomentoErrorCode.NOT_FOUND_ERROR,
        completeMessage(transportErrorDetails),
        cause,
        transportErrorDetails);
  }

  private static String completeMessage(MomentoTransportErrorDetails transportErrorDetails) {
    return transportErrorDetails
        .getGrpcErrorDetails()
        .getCacheName()
        .map(s -> MESSAGE + " Cache name: " + s)
        .orElse(MESSAGE);
  }
}
