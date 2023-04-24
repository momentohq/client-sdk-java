package momento.sdk.responses.cache.signing;

import java.util.Date;
import momento.sdk.exceptions.SdkException;

/** Response for a create signing key operation */
public interface SigningKeyCreateResponse {

  /** A successful create signing key operation. Contains the new signing key and metadata. */
  class Success implements SigningKeyCreateResponse {

    private final String keyId;
    private final String endpoint;
    private final String key;
    private final Date expiresAt;

    /**
     * Constructs a create signing key success.
     *
     * @param keyId The ID of the key.
     * @param endpoint Which endpoint the key is authorized for.
     * @param key The key.
     * @param expiresAt The date the key will expire.
     */
    public Success(String keyId, String endpoint, String key, Date expiresAt) {
      this.keyId = keyId;
      this.endpoint = endpoint;
      this.key = key;
      this.expiresAt = expiresAt;
    }

    /**
     * Gets the signing key ID.
     *
     * @return the key ID.
     */
    public String getKeyId() {
      return keyId;
    }

    /**
     * Gets the endpoint the key is authorized for.
     *
     * @return The endpoint.
     */
    public String getEndpoint() {
      return endpoint;
    }

    /**
     * Gets the signing key.
     *
     * @return the key.
     */
    public String getKey() {
      return key;
    }

    /**
     * Gets the expiration date of the signing key.
     *
     * @return the expiration date.
     */
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
  class Error extends SdkException implements SigningKeyCreateResponse {

    /**
     * Constructs a signing key creation error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
