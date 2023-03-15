package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Momento Service encountered an unexpected exception while trying to fulfill the request. */
public class InternalServerException extends MomentoServiceException {

  public InternalServerException(String message) {
    super(message);
  }

  public InternalServerException(
      String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }

  public InternalServerException(
      String message, Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, cause, transportErrorDetails);
  }
}
