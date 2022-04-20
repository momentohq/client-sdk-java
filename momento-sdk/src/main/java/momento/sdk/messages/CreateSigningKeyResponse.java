package momento.sdk.messages;

import java.util.Date;

public class CreateSigningKeyResponse {
  public CreateSigningKeyResponse(String userId, String endpoint, String key, Date expiresAt) {
    this.userId = userId;
    this.endpoint = endpoint;
    this.key = key;
    this.expiresAt = expiresAt;
  }

  public String getUserId() {
    return userId;
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

  private final String userId;
  private final String endpoint;
  private final String key;
  private final Date expiresAt;
}
