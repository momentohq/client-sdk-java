package momento.sdk.auth;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import javax.annotation.Nonnull;
import momento.sdk.exceptions.InvalidArgumentException;

/** Parses connection and authentication information from a JWT provided as a string. */
public class StringCredentialProvider implements CredentialProvider {

  private static class TokenAndEndpoints {
    public final String controlEndpoint;
    public final String cacheEndpoint;
    public final String authToken;

    public TokenAndEndpoints(String controlEndpoint, String cacheEndpoint, String authToken) {
      this.controlEndpoint = controlEndpoint;
      this.cacheEndpoint = cacheEndpoint;
      this.authToken = authToken;
    }
  }

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
    TokenAndEndpoints data;
    try {
      data = processV1Token(authToken);
    } catch (IllegalArgumentException iae) {
      // if base64 decoding fails, fall back to processing legacy token
      if (iae.getMessage().contains("base64")) {
        data = processLegacyToken(authToken);
      } else {
        throw new InvalidArgumentException(iae.getMessage(), iae);
      }
    } catch (NullPointerException e) {
      throw new InvalidArgumentException("Auth token must not be null");
    }
    this.authToken = data.authToken;
    controlEndpoint = data.controlEndpoint;
    cacheEndpoint = data.cacheEndpoint;
  }

  private TokenAndEndpoints processLegacyToken(String authToken) {
    final String unsignedAuthToken = stripAuthTokenSignature(authToken);

    final Claims claims;
    try {
      claims = Jwts.parserBuilder().build().parseClaimsJwt(unsignedAuthToken).getBody();
    } catch (Exception e) {
      throw new InvalidArgumentException("Unable to parse auth token", e);
    }

    final String controlEp = claims.get(CONTROL_ENDPOINT_CLAIM_NAME, String.class);
    if (controlEp == null) {
      throw new InvalidArgumentException("Unable to parse control endpoint from auth token");
    }

    final String cacheEp = claims.get(CACHE_ENDPOINT_CLAIM_NAME, String.class);
    if (cacheEp == null) {
      throw new InvalidArgumentException("Unable to parse cache endpoint from auth token");
    }
    return new TokenAndEndpoints(controlEp, cacheEp, authToken);
  }

  private TokenAndEndpoints processV1Token(String authToken) {
    final byte[] decodedBase64Token = Base64.getDecoder().decode(authToken);
    final String decodedString = new String(decodedBase64Token, StandardCharsets.UTF_8);
    final Type type = new TypeToken<Map<String, String>>() {}.getType();
    final Map<String, String> tokenData = new Gson().fromJson(decodedString, type);
    final String host = tokenData.get("endpoint");
    final String apiKey = tokenData.get("api_key");
    if (host == null || host.isEmpty() || apiKey == null || apiKey.isEmpty()) {
      throw new InvalidArgumentException("Unable to parse auth token");
    }
    return new TokenAndEndpoints("control." + host, "cache." + host, apiKey);
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
