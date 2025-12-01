package momento.sdk.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import momento.sdk.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class GlobalCredentialProviderTest {
  private static final String ENV_VAR_NAME = "MOMENTO_TEST_GLOBAL_API_KEY";
  private static final String TEST_API_KEY = "test_global_api_key";
  private static final String TEST_ENDPOINT = "test_endpoint";

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
}
