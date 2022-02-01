package momento.sdk.exceptions;

/** Exception when token is not provided or is invalid. */
public class AuthenticationException extends CacheServiceException {
  public AuthenticationException(String message) {
    super(message);
  }
}
