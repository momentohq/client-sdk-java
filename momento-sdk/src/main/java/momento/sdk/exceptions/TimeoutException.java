package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Requested operation did not complete in allotted time. */
public class TimeoutException extends MomentoServiceException {

  private static final String MESSAGE =
      "The client's configured timeout was exceeded; you may need to use "
          + "a Configuration with more lenient timeouts.";

  /**
   * Constructs a TimeoutException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public TimeoutException(Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(MomentoErrorCode.TIMEOUT_ERROR, MESSAGE, cause, transportErrorDetails);
  }
}
