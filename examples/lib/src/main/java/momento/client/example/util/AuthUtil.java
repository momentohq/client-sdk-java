package momento.client.example.util;

import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.exceptions.SdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Small utility class to vend credentials
 */
public class AuthUtil {
    private static final String AUTH_TOKEN_ENV_VAR = "MOMENTO_AUTH_TOKEN";
    private static final Logger logger = LoggerFactory.getLogger(AuthUtil.class);

    public static CredentialProvider getCredentials() {
        try {
            return new EnvVarCredentialProvider(AUTH_TOKEN_ENV_VAR);
        } catch (SdkException e) {
            logger.error("Unable to load credential from environment variable " + AUTH_TOKEN_ENV_VAR, e);
            throw e;
        }
    }
}
