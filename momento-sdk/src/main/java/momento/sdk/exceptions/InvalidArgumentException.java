package momento.sdk.exceptions;

/** SDK client side validation fails. */
public class InvalidArgumentException extends ClientSdkException {
  public InvalidArgumentException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidArgumentException(String message) {
    super(message);
  }
}
