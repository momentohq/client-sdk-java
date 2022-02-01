package momento.sdk.exceptions;

/** Invalid parameters sent to Momento Services. */
public class BadRequestException extends MomentoServiceException {
  public BadRequestException(String message) {
    super(message);
  }
}
