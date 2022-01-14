package momento.sdk.exceptions;

/** Exception when SDK client side validation fails. */
public class ValidationException extends ClientSdkException {
  public ValidationException(String message, Throwable cause) {
    super(message, cause);
  }

  public ValidationException(String message) {
    super(message);
  }
}
