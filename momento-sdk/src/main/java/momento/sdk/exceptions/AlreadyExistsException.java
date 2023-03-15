package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** A resource already exists. */
public class AlreadyExistsException extends MomentoServiceException {

  public AlreadyExistsException(
      String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }
}
