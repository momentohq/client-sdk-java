package momento.sdk.exceptions;

/** A resource already exists. */
public class AlreadyExistsException extends MomentoServiceException {

  public AlreadyExistsException(String message) {
    super(message);
  }
}
