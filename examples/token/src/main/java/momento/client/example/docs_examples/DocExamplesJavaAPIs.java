package momento.client.example.docs_examples;

import momento.sdk.AuthClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.accessControl.DisposableTokenScopes;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.responses.auth.GenerateDisposableTokenResponse;

public class DocExamplesJavaAPIs {

  public static void example_API_GenerateDisposableToken(AuthClient authClient) {
    final GenerateDisposableTokenResponse response =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadWrite("squirrel", "mo"), ExpiresIn.minutes(30))
            .join();
    if (response instanceof GenerateDisposableTokenResponse.Success success) {
      System.out.println("Successfully generated the disposable token: " + success.authToken());
    } else if (response instanceof GenerateDisposableTokenResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to generate disposable token: "
              + error.getErrorCode(),
          error);
    }
  }

  public static void main(String[] args) {
    try (final AuthClient authClient =
        AuthClient.builder(CredentialProvider.fromEnvVar("MOMENTO_API_KEY")).build()) {
      example_API_GenerateDisposableToken(authClient);
    }
  }
}
