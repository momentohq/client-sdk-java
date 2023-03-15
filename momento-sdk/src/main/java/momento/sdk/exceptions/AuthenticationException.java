package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Authentication token is not provided or is invalid. */
public class AuthenticationException extends MomentoServiceException {
  public AuthenticationException(
      String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }
}
