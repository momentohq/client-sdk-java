package momento.sdk.auth;

import javax.annotation.Nonnull;

import momento.sdk.auth.GlobalStringCredentialProvider;

public class GlobalEnvVarCredentialProvider extends GlobalStringCredentialProvider {

    public GlobalEnvVarCredentialProvider(@Nonnull String envVarName, @Nonnull String endpoint) {
        super(getApiKeyValueFromEnvVar(envVarName), endpoint);
    }

    private static String getApiKeyValueFromEnvVar(String envVarName) {
        if (envVarName == null || envVarName.isEmpty()) { // Check for empty env var name
            throw new InvalidArgumentException("Env var name cannot be empty");
        }

        String authToken = System.getenv(envVarName);
        if (authToken == null || authToken.isEmpty()) { // Check for empty value
            throw new InvalidArgumentException("Env var " + envVarName + " must be set");
        }
        return authToken;
    }
}