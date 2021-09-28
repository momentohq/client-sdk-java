package momento.sdk.exceptions;

/**
 * An exception to indicate that a another cache with same name already exists for this requester.
 */
public class CacheAlreadyExistsException extends CacheServiceException {

  public CacheAlreadyExistsException(String message) {
    super(message);
  }
}
