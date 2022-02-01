package momento.sdk.exceptions;

/** An exception thrown when a resource already exists. */
public class AlreadyExistsException extends MomentoServiceException {

  public AlreadyExistsException(String message) {
    super(message);
  }
}
