package momento.sdk.exceptions;

/** Operation was cancelled. */
public class CancellationException extends MomentoServiceException {
  public CancellationException(String message) {
    super(message);
  }
}
