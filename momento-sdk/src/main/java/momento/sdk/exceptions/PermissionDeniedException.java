package momento.sdk.exceptions;

/** Exception when the provided credentials did not have permissions to execute an operation. */
public class PermissionDeniedException extends MomentoServiceException {

  public PermissionDeniedException(String message) {
    super(message);
  }
}
