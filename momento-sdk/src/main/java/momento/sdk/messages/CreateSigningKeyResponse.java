package momento.sdk.messages;

import java.util.Date;
import momento.sdk.exceptions.SdkException;
import momento.sdk.exceptions.WrappedSdkException;

/** Response for a create signing key operation */
public interface CreateSigningKeyResponse {

  /** A successful create signing key operation. Contains the new signing key and metadata. */
  class Success implements CreateSigningKeyResponse {

    private final String keyId;
    private final String endpoint;
    private final String key;
    private final Date expiresAt;

    public Success(String keyId, String endpoint, String key, Date expiresAt) {
      this.keyId = keyId;
      this.endpoint = endpoint;
      this.key = key;
      this.expiresAt = expiresAt;
    }

    public String getKeyId() {
      return keyId;
    }

    public String getEndpoint() {
      return endpoint;
    }

    public String getKey() {
      return key;
    }

    public Date getExpiresAt() {
      return expiresAt;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Prints key metadata but not the key.
     */
    @Override
    public String toString() {
      return super.toString()
          + ": keyId: \""
          + getKeyId()
          + "\" endpoint: \""
          + getEndpoint()
          + "\" expiresAt: \""
          + getExpiresAt()
          + "\"";
    }
  }

  /**
   * A failed signing key creation operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends WrappedSdkException implements CreateSigningKeyResponse {

    public Error(SdkException cause) {
      super(cause);
    }
  }
}
