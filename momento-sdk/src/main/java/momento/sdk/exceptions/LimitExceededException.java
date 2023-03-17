package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Requested operation couldn't be completed because system limits were hit. */
public class LimitExceededException extends MomentoServiceException {

  private static final String MESSAGE =
      "Request rate, bandwidth, or object size exceeded the limits for this account. To resolve this error, "
          + "reduce your usage as appropriate or contact Momento to request a limit increase.";

  /**
   * Constructs a LimitExceededException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public LimitExceededException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(MomentoErrorCode.LIMIT_EXCEEDED_ERROR, MESSAGE, cause, transportErrorDetails);
  }
}
