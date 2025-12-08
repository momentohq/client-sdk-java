package momento.sdk.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import momento.sdk.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GlobalCredentialProviderTest {
  private static final String ENV_VAR_NAME = "MOMENTO_TEST_GLOBAL_API_KEY";
  private static final String TEST_API_KEY =
      "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJ0IjoiZyJ9.LloWc3qLRkBm_djlOjXE8wNSENqOay17xHLJR5XIr0cwkyhhh8w_oBaiQDktBkOvh-wKLQGUKavSQuOwXEb2_g";
  private static final String TEST_ENDPOINT = "test_endpoint";
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
  void globalKeyFromString() {
    CredentialProvider credentialProvider =
        new GlobalStringCredentialProvider(TEST_API_KEY, TEST_ENDPOINT);

    assertThat(TEST_API_KEY).isEqualTo(credentialProvider.getAuthToken());
    assertThat("cache." + TEST_ENDPOINT).isEqualTo(credentialProvider.getCacheEndpoint());
    assertThat("control." + TEST_ENDPOINT).isEqualTo(credentialProvider.getControlEndpoint());
    assertThat("token." + TEST_ENDPOINT).isEqualTo(credentialProvider.getTokenEndpoint());
    assertThat("storage." + TEST_ENDPOINT).isEqualTo(credentialProvider.getStorageEndpoint());
  }

  @Test
  void globalFromStringEmptyArguments() {
    // Test empty endpoint
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> CredentialProvider.globalKeyFromString(TEST_API_KEY, ""))
        .withMessageContaining("Endpoint must not be empty");

    // Test empty API key
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> CredentialProvider.globalKeyFromString("", TEST_ENDPOINT))
        .withMessageContaining("Auth token must not be empty");
  }

  /*
   * Java does not appear to provide a way to dynamically set environment
   * variables, cannot test the happy path
   */
  @Test
  void globalFromEnvVarNotSet() {
    // Ensure the env var is not set
    System.clearProperty(ENV_VAR_NAME);

    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(() -> CredentialProvider.globalKeyFromEnvVar(ENV_VAR_NAME, TEST_ENDPOINT))
        .withMessageContaining("Env var " + ENV_VAR_NAME + " must be set");
  }

  @Test
  void globalFromStringWithV1Token() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(
            () -> CredentialProvider.globalKeyFromString(VALID_V1_AUTH_TOKEN, TEST_ENDPOINT))
        .withMessageContaining(
            "Global API key appears to be a V1 or legacy token. "
                + "Please use CredentialProvider.fromString() instead of globalKeyFromString()");
  }

  @Test
  void globalFromStringWithLegacyToken() {
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(
            () -> CredentialProvider.globalKeyFromString(VALID_LEGACY_AUTH_TOKEN, TEST_ENDPOINT))
        .withMessageContaining(
            "Global API key appears to be a V1 or legacy token. "
                + "Please use CredentialProvider.fromString() instead of globalKeyFromString()");
  }
}
