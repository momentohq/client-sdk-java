package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Requested resource or the resource on which an operation was requested doesn't exist. */
public class NotFoundException extends MomentoServiceException {

  public NotFoundException(String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }
}
