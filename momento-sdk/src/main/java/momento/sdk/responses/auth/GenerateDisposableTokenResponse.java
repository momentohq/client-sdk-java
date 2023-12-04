package momento.sdk.responses.auth;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
import momento.sdk.auth.accessControl.ExpiresAt;
import momento.sdk.exceptions.SdkException;
import momento.token._GenerateDisposableTokenResponse;

public interface GenerateDisposableTokenResponse {

  class Success implements GenerateDisposableTokenResponse {
    private final String authToken;
    private final String endpoint;
    private final ExpiresAt expiresAt;

    public Success(_GenerateDisposableTokenResponse response) {
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put("endpoint", response.getEndpoint());
      jsonMap.put("api_key", response.getApiKey());
      String jsonString = new Gson().toJson(jsonMap);
      byte[] jsonBytes = jsonString.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      authToken = java.util.Base64.getEncoder().encodeToString(jsonBytes);
      endpoint = response.getEndpoint();
      expiresAt = ExpiresAt.fromEpoch((int) response.getValidUntil());
    }

    @Override
    public String toString() {
      int len = authToken.length();
      return authToken.substring(0, 10) + "..." + authToken.substring(len - 10, len);
    }

    public String authToken() {
      return authToken;
    }

    public String endpoint() {
      return endpoint;
    }

    public ExpiresAt expiresAt() {
      return expiresAt;
    }
  }

  class Error extends SdkException implements GenerateDisposableTokenResponse {
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
