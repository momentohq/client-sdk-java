package momento.sdk.exceptions;

/** Service rejected the request as the authentication credentials presented are invalid. */
public class PermissionDeniedException extends CacheServiceException {

  public PermissionDeniedException(String message) {
    super(message);
  }
}
