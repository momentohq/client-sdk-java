package momento.sdk.exceptions;

/**
 * Exception when an operation couldn't be completed because system limits were hit.
 */
public class LimitExceededException extends MomentoServiceException {
  public LimitExceededException(String message) {
    super(message);
  }
}
