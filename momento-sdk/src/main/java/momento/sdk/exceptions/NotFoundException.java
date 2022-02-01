package momento.sdk.exceptions;

/** Exception when operations are performed on a Cache that doesn't exist. */
public class NotFoundException extends CacheServiceException {

  public NotFoundException(String message) {
    super(message);
  }
}
