package momento.sdk.messages;

import java.util.Date;

/** Metadata about a signing key. */
public class SigningKey {
  private final String keyId;
  private final Date expiresAt;
  private final String endpoint;

  /**
   * Constructs a SigningKey.
   *
   * @param keyId The ID of the key.
   * @param expiresAt The date the key will expire.
   * @param endpoint Which endpoint the key is authorized for.
   */
  public SigningKey(String keyId, Date expiresAt, String endpoint) {
    this.keyId = keyId;
    this.expiresAt = expiresAt;
    this.endpoint = endpoint;
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
   * Gets the expiration date of the signing key.
   *
   * @return the expiration date.
   */
  public Date getExpiresAt() {
    return expiresAt;
  }

  /**
   * Gets the endpoint the key is authorized for.
   *
   * @return The endpoint.
   */
  public String getEndpoint() {
    return endpoint;
  }
}
