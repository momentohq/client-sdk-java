package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Insufficient permissions to execute an operation. */
public class PermissionDeniedException extends MomentoServiceException {

  public PermissionDeniedException(
      String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }
}
