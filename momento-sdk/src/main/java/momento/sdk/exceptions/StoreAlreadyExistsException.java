package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** A resource already exists. */
public class StoreAlreadyExistsException extends MomentoServiceException {

  private static final String MESSAGE =
      "A store with the specified name already exists. To resolve this error, "
          + "either delete the existing store and make a new one, or use a different name.";

  /**
   * Constructs an StoreAlreadyExistsException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public StoreAlreadyExistsException(
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
