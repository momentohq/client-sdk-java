package momento.client.example.util;

import momento.sdk.auth.CredentialProvider;
import momento.sdk.exceptions.SdkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

/** Small utility class to vend credentials */
public class AuthUtil {
  private static final String AUTH_TOKEN_VAR = "MOMENTO_AUTH_TOKEN";
  private static final Logger logger = LoggerFactory.getLogger(AuthUtil.class);

  /** Helper method to vend Momento credentials through auth token stored in environment variable */
  public static CredentialProvider getCredentialsFromEnvironmentVariable() {
    try {
      return CredentialProvider.fromEnvVar(AUTH_TOKEN_VAR);
    } catch (SdkException e) {
      logger.error("Unable to load credential from environment variable " + AUTH_TOKEN_VAR, e);
      throw e;
    }
  }

  /** Helper method to vend Momento credentials through auth token stored in secrets manager */
  public static CredentialProvider getCredentialsFromSecretsManagerAuthToken() {
    final Region region = Region.of("us-east-1");

    // Create a Secrets Manager client
    final SecretsManagerClient client =
        SecretsManagerClient.builder()
            .region(region)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    final GetSecretValueRequest getSecretValueRequest =
        GetSecretValueRequest.builder().secretId(AUTH_TOKEN_VAR).build();

    final GetSecretValueResponse getSecretValueResponse;

    try {
      getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
    } catch (Exception e) {
      // For a list of exceptions thrown, see
      // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
      throw e;
    }

    final String secret = getSecretValueResponse.secretString();
    try {
      return CredentialProvider.fromString(secret);
    } catch (SdkException e) {
      logger.error("Unable to load credential from secrets manager, key: " + AUTH_TOKEN_VAR, e);
      throw e;
    }
  }
}
