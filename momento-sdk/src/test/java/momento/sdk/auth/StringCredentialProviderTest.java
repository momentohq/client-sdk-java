package momento.sdk.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import momento.sdk.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.Test;

class StringCredentialProviderTest {

  private static final String CONTROL_ENDPOINT = "control.example.com";
  private static final String CACHE_ENDPOINT = "cache.example.com";

  private static final String VALID_AUTH_TOKEN =
      "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJzcXVpcnJlbCIsImNwIjoiY29udHJvbC5leGFtcGxlL"
          + "mNvbSIsImMiOiJjYWNoZS5leGFtcGxlLmNvbSJ9.YY7RSMBCpMRs_qgbNkW0PYC2eX-M"
          + "ukLixLWJyvBpnMVaOba-OV0G5jgNmNbtn4zaLT8tlEncV6wQ_CkTI_PvoA";
  public static final String MISSING_CONTROL_TOKEN =
      "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJzcXVpcnJlbCIsImMiOiJjYWNoZS5leGFtcGxlLmNvb"
          + "SJ9.RzLpBXut4s0fEXHtVIYVNb6Z8tiHSP9iu2j6OJpJHDksNXuOgTVFlMyG4V3gvMLM"
          + "UwQmgtov-U9pMbaghQnr-Q";
  public static final String MISSING_CACHE_TOKEN =
      "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJzcXVpcnJlbCIsImNwIjoiY29udHJvbC5leGFtcGxlL"
          + "mNvbSJ9.obg5-runV-bdp0ZTV_2DGDFdRfc6aIRHaSBGbK3QaACPXwF6e8ghBYg2LDXX"
          + "OWgbdpy6wEfDVIPgYZ0yXxVqvg";

  @Test
  public void testCredentialProviderNullToken() {
    //noinspection DataFlowIssue
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> new StringCredentialProvider(null))
        .withMessageContaining("null");
  }

  @Test
  public void testCredentialProviderUnparsableToken() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> new StringCredentialProvider("this isn't a real JWT"))
        .withMessageContaining("Malformed");
  }

  @Test
  public void testCredentialProviderNoControlEndpoint() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> new StringCredentialProvider(MISSING_CONTROL_TOKEN))
        .withMessageContaining("control endpoint");
  }

  @Test
  public void testCredentialProviderNoCacheEndpoint() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> new StringCredentialProvider(MISSING_CACHE_TOKEN))
        .withMessageContaining("cache endpoint");
  }

  @Test
  public void testCredentialProviderHappyPath() {
    assertThat(new StringCredentialProvider(VALID_AUTH_TOKEN))
        .satisfies(
            provider -> {
              assertThat(provider.getAuthToken()).isEqualTo(VALID_AUTH_TOKEN);
              assertThat(provider.getControlEndpoint()).isEqualTo(CONTROL_ENDPOINT);
              assertThat(provider.getCacheEndpoint()).isEqualTo(CACHE_ENDPOINT);
            });
  }
}
