package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Insufficient permissions to execute an operation. */
public class PermissionDeniedException extends MomentoServiceException {

  private static final String MESSAGE =
      "Insufficient permissions to perform an operation on a cache.";

  /**
   * Constructs a PermissionDeniedException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public PermissionDeniedException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(MomentoErrorCode.PERMISSION_ERROR, MESSAGE, cause, transportErrorDetails);
  }
}
