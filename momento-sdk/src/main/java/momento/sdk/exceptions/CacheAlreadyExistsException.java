package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** A resource already exists. */
public class CacheAlreadyExistsException extends MomentoServiceException {

  private static final String MESSAGE =
      "A cache with the specified name already exists. To resolve this error, "
          + "either delete the existing cache and make a new one, or use a different name.";

  /**
   * Constructs a CacheAlreadyExistsException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public CacheAlreadyExistsException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(
        MomentoErrorCode.ALREADY_EXISTS_ERROR,
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
