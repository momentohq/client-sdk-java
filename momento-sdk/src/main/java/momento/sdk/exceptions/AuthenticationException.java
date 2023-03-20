package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Authentication token is not provided or is invalid. */
public class AuthenticationException extends MomentoServiceException {

  private static final String MESSAGE =
      "Invalid authentication credentials to connect to the cache service.";

  /**
   * Constructs an AuthenticationException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public AuthenticationException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(MomentoErrorCode.AUTHENTICATION_ERROR, MESSAGE, cause, transportErrorDetails);
  }
}
