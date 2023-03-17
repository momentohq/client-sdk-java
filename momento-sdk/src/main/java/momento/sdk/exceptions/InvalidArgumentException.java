package momento.sdk.exceptions;

/** SDK client side validation fails. */
public class InvalidArgumentException extends ClientSdkException {

  private static final String MESSAGE_PREFIX = "Invalid argument passed to Momento client: ";

  /**
   * Constructs an InvalidArgumentException with a detail message and a cause.
   *
   * @param message the detail message.
   * @param cause the cause.
   */
  public InvalidArgumentException(String message, Throwable cause) {
    super(MomentoErrorCode.INVALID_ARGUMENT_ERROR, MESSAGE_PREFIX + message, cause);
  }

  /**
   * Constructs an InvalidArgumentException with a detail message.
   *
   * @param message the detail message.
   */
  public InvalidArgumentException(String message) {
    super(MomentoErrorCode.INVALID_ARGUMENT_ERROR, MESSAGE_PREFIX + message);
  }
}
