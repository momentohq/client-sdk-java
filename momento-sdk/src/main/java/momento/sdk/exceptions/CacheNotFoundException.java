package momento.sdk.exceptions;

/** Exception when operations are performed on a Cache that doesn't exist. */
public class CacheNotFoundException extends CacheServiceException {

  public CacheNotFoundException(String message) {
    super(message);
  }
}
