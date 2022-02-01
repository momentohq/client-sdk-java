package momento.sdk.exceptions;

/** Exception when requested operation did not complete in allotted time. */
public class TimeoutException extends MomentoServiceException {
  public TimeoutException(String message) {
    super(message);
  }
}
