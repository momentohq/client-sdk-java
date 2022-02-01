package momento.sdk.exceptions;

/** Requested resource or the resource on which an operation was requested doesn't exist. */
public class NotFoundException extends MomentoServiceException {

  public NotFoundException(String message) {
    super(message);
  }
}
