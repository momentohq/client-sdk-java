package momento.sdk;

import static org.junit.jupiter.api.Assertions.*;

import momento.sdk.exceptions.ClientSdkException;
import org.junit.jupiter.api.Test;

final class AuthTokenParserTest {

  // These secrets have botched up signature section, so should be okay to have them in source
  // control.
  private static final String TEST_AUTH_TOKEN_NO_ENDPOINT =
      "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiJ9.ZOgkTs";
  private static final String TEST_AUTH_TOKEN_ENDPOINT =
      "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJzcXVpcnJlbCIsImNwIjoiY29udHJvbCBwbGFuZSBlbmRwb2ludCIsImMiOiJkYXRhIHBsYW5lIGVuZHBvaW50In0.zsTsEXFawetTCZI";

  @Test
  public void shouldParseAuthTokenWithNoEndpoints() {
    AuthTokenParser.Claims claims = AuthTokenParser.parse(TEST_AUTH_TOKEN_NO_ENDPOINT);
    assertFalse(claims.cacheEndpoint().isPresent());
    assertFalse(claims.controlEndpoint().isPresent());
  }

  @Test
  public void shouldParseAuthTokenWithEndpoints() {
    AuthTokenParser.Claims claims = AuthTokenParser.parse(TEST_AUTH_TOKEN_ENDPOINT);
    assertEquals("control plane endpoint", claims.controlEndpoint().get());
    assertEquals("data plane endpoint", claims.cacheEndpoint().get());
  }

  @Test
  public void throwExceptionWhenAuthTokenEmptyOrNull() {
    assertThrows(ClientSdkException.class, () -> AuthTokenParser.parse(null));
    assertThrows(ClientSdkException.class, () -> AuthTokenParser.parse("   "));
  }

  @Test
  public void throwExceptionForInvalidClaimsToken() {
    assertThrows(ClientSdkException.class, () -> AuthTokenParser.parse("abcd.effh.jdjjdjdj"));
  }

  @Test
  public void throwExceptionForMalformedToken() {
    assertThrows(ClientSdkException.class, () -> AuthTokenParser.parse("abcd"));
  }
}
