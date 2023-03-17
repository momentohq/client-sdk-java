package momento.sdk.messages;

import java.util.List;
import java.util.stream.Collectors;
import momento.sdk.exceptions.SdkException;

/** Response for a list signing keys operation. */
public interface ListSigningKeysResponse {

  /** A successful list signing keys operation. Contains the discovered signing keys. */
  class Success implements ListSigningKeysResponse {

    private final List<SigningKey> signingKeys;

    /**
     * Constructs a list signing keys success with a list of found keys
     *
     * @param signingKeys the retrieved keys.
     */
    public Success(List<SigningKey> signingKeys) {
      this.signingKeys = signingKeys;
    }

    /**
     * Returns the retrieved signing keys.
     *
     * @return the keys.
     */
    public List<SigningKey> signingKeys() {
      return signingKeys;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Limits the keys to 5 to bound the size of the string. Prints the key ids instead of the
     * keys.
     */
    @Override
    public String toString() {
      return super.toString()
          + ": keys: "
          + signingKeys().stream()
              .map(SigningKey::getKeyId)
              .limit(5)
              .collect(Collectors.joining("\", \"", "\"", "\"..."));
    }
  }

  /**
   * A failed list signing keys operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements ListSigningKeysResponse {

    /**
     * Constructs a signing key list error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
