package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Requested operation did not complete in allotted time. */
public class TimeoutException extends MomentoServiceException {
  public TimeoutException(String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }
}
