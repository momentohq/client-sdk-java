package momento.sdk.exceptions;

/** Requested operation did not complete in allotted time. */
public class TimeoutException extends MomentoServiceException {
  public TimeoutException(String message) {
    super(message);
  }
}
