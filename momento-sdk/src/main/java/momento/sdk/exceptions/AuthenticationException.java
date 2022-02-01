package momento.sdk.exceptions;

/** Authentication token is not provided or is invalid. */
public class AuthenticationException extends MomentoServiceException {
  public AuthenticationException(String message) {
    super(message);
  }
}
