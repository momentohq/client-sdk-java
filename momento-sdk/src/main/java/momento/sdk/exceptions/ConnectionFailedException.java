package momento.sdk.exceptions;

public class ConnectionFailedException extends MomentoServiceException {

  /**
   * Constructs a ConnectionFailedException with a message.
   *
   * @param message the message/cause for exception.
   */
  public ConnectionFailedException(final String message) {
    super(MomentoErrorCode.CONNECTION, message);
  }

  /**
   * Constructs a ConnectionFailedException with a message and error details.
   *
   * @param message the message/cause for exception.
   * @param cause the error details.
   */
  public ConnectionFailedException(final String message, final Throwable cause) {
    super(MomentoErrorCode.CONNECTION, message, cause);
  }
}
