package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Momento Service encountered an unexpected exception while trying to fulfill the request. */
public class InternalServerException extends MomentoServiceException {

  private static final String MESSAGE =
      "An unexpected error occurred while trying to fulfill the request; please contact Momento.";

  /**
   * Constructs an InternalServerException with a detail message.
   *
   * @param message the detail message.
   */
  public InternalServerException(String message) {
    super(MomentoErrorCode.INTERNAL_SERVER_ERROR, MESSAGE + " " + message);
  }

  /**
   * Constructs an InternalServerException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public InternalServerException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(MomentoErrorCode.INTERNAL_SERVER_ERROR, MESSAGE, cause, transportErrorDetails);
  }
}
