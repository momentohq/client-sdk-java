package momento.sdk.exceptions;

/** Exception when the requested resource or the resource on which an operation is being performed doesn't exist. */
public class NotFoundException extends MomentoServiceException {

  public NotFoundException(String message) {
    super(message);
  }
}
