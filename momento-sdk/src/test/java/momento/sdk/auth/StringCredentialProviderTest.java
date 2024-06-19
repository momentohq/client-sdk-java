package momento.sdk.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import momento.sdk.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.Test;

class StringCredentialProviderTest {

  private static final String CONTROL_ENDPOINT_LEGACY = "control.example.com";
  private static final String CACHE_ENDPOINT_LEGACY = "cache.example.com";
  private static final String CONTROL_ENDPOINT_V1 = "control.test.momentohq.com";
  private static final String CACHE_ENDPOINT_V1 = "cache.test.momentohq.com";

  // Test tokens are all fake and nonfunctional.
  private static final String VALID_LEGACY_AUTH_TOKEN =
      "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJzcXVpcnJlbCIsImNwIjoiY29udHJvbC5leGFtcGxlL"
          + "mNvbSIsImMiOiJjYWNoZS5leGFtcGxlLmNvbSJ9.YY7RSMBCpMRs_qgbNkW0PYC2eX-M"
          + "ukLixLWJyvBpnMVaOba-OV0G5jgNmNbtn4zaLT8tlEncV6wQ_CkTI_PvoA";
  private static final String VALID_V1_AUTH_TOKEN =
      "eyJhcGlfa2V5IjogImV5SjBlWEFpT2lKS1YxUWlMQ0poYkdjaU9pSklVekkxTmlKOS5leUpwYz"
          + "NNaU9pSlBibXhwYm1VZ1NsZFVJRUoxYVd4a1pYSWlMQ0pwWVhRaU9qRTJOemd6TURVNE1U"
          + "SXNJbVY0Y0NJNk5EZzJOVFV4TlRReE1pd2lZWFZrSWpvaUlpd2ljM1ZpSWpvaWFuSnZZMn"
          + "RsZEVCbGVHRnRjR3hsTG1OdmJTSjkuOEl5OHE4NExzci1EM1lDb19IUDRkLXhqSGRUOFVD"
          + "SXV2QVljeGhGTXl6OCIsICJlbmRwb2ludCI6ICJ0ZXN0Lm1vbWVudG9ocS5jb20ifQ==";
  private static final String VALID_V1_API_KEY =
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLC"
          + "JpYXQiOjE2NzgzMDU4MTIsImV4cCI6NDg2NTUxNTQxMiwiYXVkIjoiIiwic3ViIjoianJvY"
          + "2tldEBleGFtcGxlLmNvbSJ9.8Iy8q84Lsr-D3YCo_HP4d-xjHdT8UCIuvAYcxhFMyz8";
  private static final String TEST_V1_MISSING_ENDPOINT =
      "eyJhcGlfa2V5IjogImV5SmxibVJ3YjJsdWRDS"
          + "TZJbU5sYkd3dE5DMTFjeTEzWlhOMExUSXRNUzV3Y205a0xtRXViVzl0Wlc1MGIyaHhMbU52Yl"
          + "NJc0ltRndhVjlyWlhraU9pSmxlVXBvWWtkamFVOXBTa2xWZWtreFRtbEtPUzVsZVVwNlpGZEp"
          + "hVTlwU25kYVdGSnNURzFrYUdSWVVuQmFXRXBCV2pJeGFHRlhkM1ZaTWpsMFNXbDNhV1J0Vm5s"
          + "SmFtOTRabEV1VW5OMk9GazVkRE5KVEMwd1RHRjZiQzE0ZDNaSVZESmZZalJRZEhGTlVVMDVRV"
          + "3hhVlVsVGFrbENieUo5In0=";
  private static final String TEST_V1_MISSING_API_KEY = "eyJlbmRwb2ludCI6ICJhLmIuY29tIn0=";
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
  public void testCredentialProviderLegacyTokenHappyPath() {
    assertThat(new StringCredentialProvider(VALID_LEGACY_AUTH_TOKEN))
        .satisfies(
            provider -> {
              assertThat(provider.getAuthToken()).isEqualTo(VALID_LEGACY_AUTH_TOKEN);
              assertThat(provider.getControlEndpoint()).isEqualTo(CONTROL_ENDPOINT_LEGACY);
              assertThat(provider.getCacheEndpoint()).isEqualTo(CACHE_ENDPOINT_LEGACY);
            });
  }

  @Test
  public void testCredentialProviderLegacyTokenOverrideEndpointsHappyPath() {
    assertThat(
            new StringCredentialProvider(
                VALID_LEGACY_AUTH_TOKEN,
                "my.control.host",
                "my.cache.host",
                "my.storage.host",
                "my.token.host"))
        .satisfies(
            provider -> {
              assertThat(provider.getAuthToken()).isEqualTo(VALID_LEGACY_AUTH_TOKEN);
              assertThat(provider.getControlEndpoint()).isEqualTo("my.control.host");
              assertThat(provider.getCacheEndpoint()).isEqualTo("my.cache.host");
              assertThat(provider.getStorageEndpoint()).isEqualTo("my.storage.host");
            });
  }

  @Test
  public void testCredentialProviderV1TokenHappyPath() {
    assertThat(new StringCredentialProvider(VALID_V1_AUTH_TOKEN))
        .satisfies(
            provider -> {
              assertThat(provider.getAuthToken()).isEqualTo(VALID_V1_API_KEY);
              assertThat(provider.getControlEndpoint()).isEqualTo(CONTROL_ENDPOINT_V1);
              assertThat(provider.getCacheEndpoint()).isEqualTo(CACHE_ENDPOINT_V1);
            });
  }

  @Test
  public void testCredentialProviderV1TokenOverrideEndpointsHappyPath() {
    assertThat(
            new StringCredentialProvider(
                VALID_V1_AUTH_TOKEN,
                "my.control.host",
                "my.cache.host",
                "my.storage.host",
                "my.token.host"))
        .satisfies(
            provider -> {
              assertThat(provider.getAuthToken()).isEqualTo(VALID_V1_API_KEY);
              assertThat(provider.getControlEndpoint()).isEqualTo("my.control.host");
              assertThat(provider.getCacheEndpoint()).isEqualTo("my.cache.host");
              assertThat(provider.getStorageEndpoint()).isEqualTo("my.storage.host");
            });
  }

  @Test
  public void testCredentialProviderV1MissingEndpoint() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> new StringCredentialProvider(TEST_V1_MISSING_ENDPOINT))
        .withMessageContaining("parse auth token");
  }

  @Test
  public void testCredentialProviderV1MissingApiKey() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> new StringCredentialProvider(TEST_V1_MISSING_API_KEY))
        .withMessageContaining("parse auth token");
  }
}
