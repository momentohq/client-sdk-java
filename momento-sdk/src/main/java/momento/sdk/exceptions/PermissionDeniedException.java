package momento.sdk.exceptions;

/** Insufficient permissions to execute an operation. */
public class PermissionDeniedException extends MomentoServiceException {

  public PermissionDeniedException(String message) {
    super(message);
  }
}
