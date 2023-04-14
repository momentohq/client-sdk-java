package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Base type for all the exceptions resulting from invalid interactions with Momento Services. */
public class MomentoServiceException extends SdkException {

  /**
   * Constructs a MomentoServiceException with an error code and a detail message.
   *
   * @param errorCode the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   * @param message the detail message.
   */
  public MomentoServiceException(MomentoErrorCode errorCode, String message) {
    super(errorCode, message);
  }

  /**
   * Constructs a MomentoServiceException with an error code, a detail message, and error details.
   *
   * @param errorCode the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   * @param message the detail message.
   * @param transportErrorDetails details about the request and error.
   */
  public MomentoServiceException(
      MomentoErrorCode errorCode,
      String message,
      MomentoTransportErrorDetails transportErrorDetails) {
    super(errorCode, message, transportErrorDetails);
  }

  /**
   * Constructs a MomentoServiceException with an error code, a detail message, a cause, and error
   * details.
   *
   * @param errorCode the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   * @param message the detail message.
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public MomentoServiceException(
      MomentoErrorCode errorCode,
      String message,
      Throwable cause,
      MomentoTransportErrorDetails transportErrorDetails) {
    super(errorCode, message, cause, transportErrorDetails);
  }
}
