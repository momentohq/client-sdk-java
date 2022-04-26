package momento.sdk.messages;

import java.util.Date;

public class SigningKey {
  private final String keyId;
  private final Date expiresAt;
  private final String endpoint;

  public SigningKey(String keyId, Date expiresAt, String endpoint) {
    this.keyId = keyId;
    this.expiresAt = expiresAt;
    this.endpoint = endpoint;
  }

  public String getKeyId() {
    return keyId;
  }

  public Date getExpiresAt() {
    return expiresAt;
  }

  public String getEndpoint() {
    return endpoint;
  }
}
