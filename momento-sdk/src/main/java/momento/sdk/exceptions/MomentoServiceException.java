package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Base type for all the exceptions resulting from invalid interactions with Momento Services. */
public class MomentoServiceException extends SdkException {

  public MomentoServiceException(String message) {
    super(message);
  }

  public MomentoServiceException(
      String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }

  public MomentoServiceException(
      String message, Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, cause, transportErrorDetails);
  }
}
