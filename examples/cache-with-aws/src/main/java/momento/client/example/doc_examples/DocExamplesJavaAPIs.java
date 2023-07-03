package momento.client.example.doc_examples;

import momento.sdk.auth.CredentialProvider;
import momento.sdk.exceptions.SdkException;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class DocExamplesJavaAPIs {

  public static void example_API_retrieveAuthTokenFromSecretsManager() {
    final Region region = Region.of("us-east-1");

    // Create a Secrets Manager client
    final SecretsManagerClient client =
        SecretsManagerClient.builder()
            .region(region)
            // make sure to configure aws credentials in order to use the default provider
            // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    final GetSecretValueRequest getSecretValueRequest =
        GetSecretValueRequest.builder().secretId("AUTH_TOKEN_SECRET_NAME").build();

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
      // store variable here
      CredentialProvider.fromString(secret);
    } catch (SdkException e) {
      throw new RuntimeException(
          "An error occured while parsing the secrets manager vended" + " authentication token", e);
    }
  }
}
