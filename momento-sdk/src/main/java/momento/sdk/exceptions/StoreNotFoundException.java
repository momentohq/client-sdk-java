package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** The store requested doesn't exist. */
public class StoreNotFoundException extends MomentoServiceException {

  private static final String MESSAGE =
      "A store with the specified name does not exist. To resolve this error, "
          + "make sure you have created the store before attempting to use it.";

  /**
   * Constructs a StoreNotFoundException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public StoreNotFoundException(
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
        .map(s -> MESSAGE + " Store name: " + s)
        .orElse(MESSAGE);
  }
}
