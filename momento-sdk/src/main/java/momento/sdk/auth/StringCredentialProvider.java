package momento.sdk.auth;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import javax.annotation.Nonnull;
import momento.sdk.exceptions.InvalidArgumentException;

/** Parses connection and authentication information from a JWT provided as a string. */
public class StringCredentialProvider implements CredentialProvider {

  private static final String CONTROL_ENDPOINT_CLAIM_NAME = "cp";
  private static final String CACHE_ENDPOINT_CLAIM_NAME = "c";

  private final String authToken;
  private final String controlEndpoint;
  private final String cacheEndpoint;

  /**
   * Parses connection and authentication information from the given JWT.
   *
   * @param authToken a Momento JWT.
   */
  public StringCredentialProvider(@Nonnull String authToken) {
    this.authToken = authToken;

    final String unsignedAuthToken = stripAuthTokenSignature(authToken);

    final Claims claims;
    try {
      claims = Jwts.parserBuilder().build().parseClaimsJwt(unsignedAuthToken).getBody();
    } catch (Exception e) {
      throw new InvalidArgumentException("Unable to parse auth token", e);
    }

    controlEndpoint = claims.get(CONTROL_ENDPOINT_CLAIM_NAME, String.class);
    if (controlEndpoint == null) {
      throw new InvalidArgumentException("Unable to parse control endpoint from auth token");
    }

    cacheEndpoint = claims.get(CACHE_ENDPOINT_CLAIM_NAME, String.class);
    if (cacheEndpoint == null) {
      throw new InvalidArgumentException("Unable to parse cache endpoint from auth token");
    }
  }

  private String stripAuthTokenSignature(String authToken) {
    if (authToken == null) {
      throw new InvalidArgumentException("Auth token must not be null");
    }

    // https://github.com/jwtk/jjwt/issues/280
    final String[] splitToken = authToken.split("\\.");
    if (splitToken.length < 2) {
      throw new InvalidArgumentException("Malformed auth token");
    }
    return splitToken[0] + "." + splitToken[1] + ".";
  }

  @Override
  public String getAuthToken() {
    return authToken;
  }

  @Override
  public String getControlEndpoint() {
    return controlEndpoint;
  }

  @Override
  public String getCacheEndpoint() {
    return cacheEndpoint;
  }
}
