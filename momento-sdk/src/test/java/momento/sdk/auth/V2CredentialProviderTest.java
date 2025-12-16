package momento.sdk.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import momento.sdk.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class V2CredentialProviderTest {

  private static final String ENV_VAR_NAME = "MOMENTO_TEST_GLOBAL_API_KEY";
  private static final String TEST_V2_API_KEY =
      "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9.eyJqdGkiOiIwMTliMjM0MC0xNDEyLTc1NzItOTAwMS00M2VlNjE1NDM0MzIiLCJ0IjoiZyIsImV4cCI6MTc2NTkyOTYwMH0.UcKw5i0QPMHS4ajEcotsyHqDhIK6IpBRrFiVayS_blBBe3TcZFtjKEij9SkP1mU3eWy_Kw-S9zeQ_Eh5eT7owA";
  private static final String TEST_ENDPOINT = "test_endpoint";
  private static final String ENDPOINT_ENV_VAR = "MOMENTO_TEST_ENDPOINT";
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

  @AfterEach
  void cleanupEnvironment() {
    System.clearProperty(ENV_VAR_NAME);
  }

  @Test
  void fromApiKeyV2() {
    CredentialProvider credentialProvider =
        new ApiKeyV2CredentialProvider(TEST_V2_API_KEY, TEST_ENDPOINT);

    assertThat(TEST_V2_API_KEY).isEqualTo(credentialProvider.getAuthToken());
    assertThat("cache." + TEST_ENDPOINT).isEqualTo(credentialProvider.getCacheEndpoint());
    assertThat("control." + TEST_ENDPOINT).isEqualTo(credentialProvider.getControlEndpoint());
    assertThat("token." + TEST_ENDPOINT).isEqualTo(credentialProvider.getTokenEndpoint());
    assertThat("storage." + TEST_ENDPOINT).isEqualTo(credentialProvider.getStorageEndpoint());
  }

  @Test
  void fromApiKeyV2EmptyArguments() {
    // Test empty endpoint
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> CredentialProvider.fromApiKeyV2(TEST_V2_API_KEY, ""))
        .withMessageContaining("Endpoint must not be empty");

    // Test empty API key
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> CredentialProvider.fromApiKeyV2("", TEST_ENDPOINT))
        .withMessageContaining("API key must not be empty");
  }

  /*
   * Java does not appear to provide a way to dynamically set environment
   * variables, cannot test the happy path
   */
  @Test
  void fromEnvVarV2NotSet() {
    // Ensure the env var is not set
    System.clearProperty(ENV_VAR_NAME);

    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> CredentialProvider.fromEnvVarV2(ENV_VAR_NAME, ENDPOINT_ENV_VAR))
        .withMessageContaining("Env var " + ENV_VAR_NAME + " must be set");
  }

  @Test
  void fromApiKeyV2WithV1Token() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> CredentialProvider.fromApiKeyV2(VALID_V1_AUTH_TOKEN, TEST_ENDPOINT))
        .withMessageContaining(
            "Received an invalid v2 API key. Did you mean to use `fromString()` or `fromEnvVar()` with a legacy key instead?");
  }

  @Test
  void fromApiKeyV2WithLegacyToken() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> CredentialProvider.fromApiKeyV2(VALID_LEGACY_AUTH_TOKEN, TEST_ENDPOINT))
        .withMessageContaining(
            "Received an invalid v2 API key. Did you mean to use `fromString()` or `fromEnvVar()` with a legacy key instead?");
  }

  @Test
  void fromDisposableTokenWithV2Key() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> CredentialProvider.fromDisposableToken(TEST_V2_API_KEY))
        .withMessageContaining(
            "Received a v2 API key. Are you using the correct key? Or did you mean to use `fromApiKeyV2()` or `fromEnvVarV2()` instead?");
  }
}
