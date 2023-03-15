package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Requested operation couldn't be completed because system limits were hit. */
public class LimitExceededException extends MomentoServiceException {
  public LimitExceededException(
      String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }
}
