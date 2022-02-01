package momento.sdk.exceptions;

/**
 * An exception to indicate that a another cache with same name already exists for this requester.
 */
public class AlreadyExistsException extends CacheServiceException {

  public AlreadyExistsException(String message) {
    super(message);
  }
}
