package momento.sdk.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import momento.sdk.auth.CredentialProvider;
import momento.sdk.exceptions.InvalidArgumentException;
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
    void globalKeyFromEnvVar() {
        System.setProperty(ENV_VAR_NAME, TEST_API_KEY);

        CredentialProvider credentialProvider = CredentialProvider.globalKeyFromEnvVar(ENV_VAR_NAME, TEST_ENDPOINT);

        assertEquals(TEST_API_KEY, credentialProvider.getAuthToken());
        assertEquals("cache." + TEST_ENDPOINT, credentialProvider.getCacheEndpoint());
        assertEquals("control." + TEST_ENDPOINT, credentialProvider.getControlEndpoint());
        assertEquals("token." + TEST_ENDPOINT, credentialProvider.getTokenEndpoint());
        assertEquals("storage." + TEST_ENDPOINT, credentialProvider.getStorageEndpoint());
    }

    @Test
    void globalKeyFromString() {
        CredentialProvider credentialProvider = CredentialProvider.globalKeyFromString(TEST_API_KEY, TEST_ENDPOINT);

        assertEquals(TEST_API_KEY, credentialProvider.getAuthToken());
        assertEquals("cache." + TEST_ENDPOINT, credentialProvider.getCacheEndpoint());
        assertEquals("control." + TEST_ENDPOINT, credentialProvider.getControlEndpoint());
        assertEquals("token." + TEST_ENDPOINT, credentialProvider.getTokenEndpoint());
        assertEquals("storage." + TEST_ENDPOINT, credentialProvider.getStorageEndpoint());
    }

    @Test
    void globalFromStringEmptyArguments() {
        // Test empty endpoint
        IllegalArgumentException emptyEndpointException = assertThrows(
                IllegalArgumentException.class,
                () -> CredentialProvider.globalKeyFromString(TEST_API_KEY, ""));
        assertEquals("Endpoint must not be empty", emptyEndpointException.getMessage());

        // Test empty API key
        IllegalArgumentException emptyKeyException = assertThrows(
                IllegalArgumentException.class,
                () -> CredentialProvider.globalKeyFromString("", TEST_ENDPOINT));
        assertEquals("Auth token string cannot be empty", emptyKeyException.getMessage());
    }

    @Test
    void globalFromEnvVarEmptyArguments() {
        System.setProperty(ENV_VAR_NAME, TEST_API_KEY);

        // Test empty endpoint
        IllegalArgumentException emptyEndpointException = assertThrows(
                IllegalArgumentException.class,
                () -> CredentialProvider.globalKeyFromEnvVar(ENV_VAR_NAME, ""));
        assertEquals("Endpoint must not be empty", emptyEndpointException.getMessage());

        // Test empty env var name
        IllegalArgumentException emptyEnvVarNameException = assertThrows(
                IllegalArgumentException.class,
                () -> CredentialProvider.globalKeyFromEnvVar("", TEST_ENDPOINT));
        assertEquals("Env var name cannot be empty", emptyEnvVarNameException.getMessage());

        // Test empty env var value
        System.setProperty(ENV_VAR_NAME, "");
        IllegalArgumentException emptyEnvVarException = assertThrows(
                IllegalArgumentException.class,
                () -> CredentialProvider.globalKeyFromEnvVar(ENV_VAR_NAME, TEST_ENDPOINT));
        assertEquals("Env var " + envVarName + " must be set", emptyEnvVarException.getMessage());
    }

    @Test
    void globalFromEnvVarNotSet() {
        // Ensure the env var is not set
        System.clearProperty(ENV_VAR_NAME);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> CredentialProvider.globalKeyFromEnvVar(ENV_VAR_NAME, TEST_ENDPOINT));
        assertEquals("Env var " + ENV_VAR_NAME + " must be set", exception.getMessage());
    }
}
