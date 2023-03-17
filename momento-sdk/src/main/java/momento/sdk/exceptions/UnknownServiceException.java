package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Service returned an unknown response. */
public class UnknownServiceException extends MomentoServiceException {

  private static final String MESSAGE =
      "The service returned an unknown response; please contact Momento.";

  /**
   * Constructs an UnknownServiceException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public UnknownServiceException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(MomentoErrorCode.UNKNOWN_SERVICE_ERROR, MESSAGE, cause, transportErrorDetails);
  }
}
