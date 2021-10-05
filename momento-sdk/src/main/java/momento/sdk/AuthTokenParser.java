package momento.sdk;

import io.jsonwebtoken.Header;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import java.util.Optional;
import momento.sdk.exceptions.ClientSdkException;

final class AuthTokenParser {

  private AuthTokenParser() {}

  public static Claims parse(String authToken) {
    try {
      ensurePresent(authToken);
      Jwt<Header, io.jsonwebtoken.Claims> parsed =
          Jwts.parserBuilder().build().parseClaimsJwt(tokenWithOnlyHeaderAndClaims(authToken));
      return new Claims(parsed.getBody());
    } catch (Exception e) {
      throw new ClientSdkException("Failed to parse Auth Token", e);
    }
  }

  private static void ensurePresent(String authToken) {
    if (authToken == null || authToken.isEmpty()) {
      throw new IllegalArgumentException("Malformed Auth Token.");
    }
  }

  // https://github.com/jwtk/jjwt/issues/280
  private static String tokenWithOnlyHeaderAndClaims(String authToken) {
    String[] splitToken = authToken.split("\\.");
    if (splitToken == null || splitToken.length < 2) {
      throw new IllegalArgumentException("Malformed Auth Token");
    }
    return splitToken[0] + "." + splitToken[1] + ".";
  }

  static class Claims {

    private static final String CONTROL_ENDPOINT_CLAIM_NAME = "cp";
    private static final String CACHE_ENDPOINT_CLAIM_NAME = "c";

    private Optional<String> controlEndpoint;
    private Optional<String> cacheEndpoint;

    private Claims(io.jsonwebtoken.Claims claims) {
      controlEndpoint =
          Optional.ofNullable((String) claims.getOrDefault(CONTROL_ENDPOINT_CLAIM_NAME, null));
      cacheEndpoint =
          Optional.ofNullable((String) claims.getOrDefault(CACHE_ENDPOINT_CLAIM_NAME, null));
    }

    public Optional<String> controlEndpoint() {
      return controlEndpoint;
    }

    public Optional<String> cacheEndpoint() {
      return cacheEndpoint;
    }
  }
}
