package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Operation was cancelled. */
public class CancellationException extends MomentoServiceException {

  private static final String MESSAGE =
      "The request was cancelled by the server; please contact Momento.";

  /**
   * Constructs a CancellationException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public CancellationException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(MomentoErrorCode.CANCELLED_ERROR, MESSAGE, cause, transportErrorDetails);
  }
}
