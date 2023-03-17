package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Invalid parameters sent to Momento Services. */
public class BadRequestException extends MomentoServiceException {

  private static final String MESSAGE = "The request was invalid; please contact Momento";

  /**
   * Constructs a BadRequestException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public BadRequestException(Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(MomentoErrorCode.BAD_REQUEST_ERROR, MESSAGE, cause, transportErrorDetails);
  }
}
