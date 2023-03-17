package momento.sdk.exceptions;

/** Unrecognized error. */
public class UnknownException extends MomentoServiceException {

  /**
   * Constructs an UnknownException with a detail message.
   *
   * @param message the detail message.
   */
  public UnknownException(String message) {
    super(MomentoErrorCode.UNKNOWN, message, null);
  }

  /**
   * Constructs an UnknownException with a detail message and cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public UnknownException(String message, Throwable cause) {
    super(MomentoErrorCode.UNKNOWN, message, cause, null);
  }
}
