package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Operation was cancelled. */
public class CancellationException extends MomentoServiceException {
  public CancellationException(String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }
}
