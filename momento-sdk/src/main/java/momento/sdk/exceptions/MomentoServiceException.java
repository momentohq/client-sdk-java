package momento.sdk.exceptions;

/** Base type for all the exceptions resulting from invalid interactions with Momento Services. */
public class MomentoServiceException extends SdkException {

  public MomentoServiceException(String message) {
    super(message);
  }
}
