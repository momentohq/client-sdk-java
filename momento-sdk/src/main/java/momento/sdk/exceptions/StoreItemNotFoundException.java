package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** The item requested from the store doesn't exist. */
public class StoreItemNotFoundException extends SdkException {

  private static final String MESSAGE =
      "The item requested from the store does not exist. To resolve this error, "
          + "if the requested item was expected to be found, put it in the store.";

  /**
   * Constructs a StoreItemNotFound with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public StoreItemNotFoundException(
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
