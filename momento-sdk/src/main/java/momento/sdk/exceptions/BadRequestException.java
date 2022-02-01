package momento.sdk.exceptions;

/** Represents exceptions thrown when invalid parameters to Momento Services. */
public class BadRequestException extends MomentoServiceException {
  public BadRequestException(String message) {
    super(message);
  }
}
