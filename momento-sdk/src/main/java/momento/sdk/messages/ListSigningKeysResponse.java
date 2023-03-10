package momento.sdk.messages;

import java.util.List;
import momento.sdk.exceptions.SdkException;

/** Response for a list signing keys operation. */
public interface ListSigningKeysResponse {

  /** A successful list signing keys operation. Contains the discovered signing keys. */
  class Success implements ListSigningKeysResponse {

    private final List<SigningKey> signingKeys;

    public Success(List<SigningKey> signingKeys) {
      this.signingKeys = signingKeys;
    }

    public List<SigningKey> signingKeys() {
      return signingKeys;
    }
  }

  /**
   * A failed list signing keys operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements ListSigningKeysResponse {

    public Error(SdkException cause) {
      super(cause.getMessage(), cause);
    }
  }
}
