package momento.client.example;

import java.util.ArrayList;
import java.util.List;
import momento.sdk.AuthClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.auth.accessControl.CacheItemSelector;
import momento.sdk.auth.accessControl.CacheRole;
import momento.sdk.auth.accessControl.CacheSelector;
import momento.sdk.auth.accessControl.DisposableToken;
import momento.sdk.auth.accessControl.DisposableTokenPermission;
import momento.sdk.auth.accessControl.DisposableTokenScope;
import momento.sdk.auth.accessControl.DisposableTokenScopes;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.auth.accessControl.TopicRole;
import momento.sdk.auth.accessControl.TopicSelector;
import momento.sdk.responses.auth.GenerateDisposableTokenResponse;

public class DisposableTokenExample {

  private static final String API_KEY_ENV_VAR = "MOMENTO_API_KEY";

  public static void main(String[] args) throws Exception {
    printStartBanner();

    final CredentialProvider credentialProvider = new EnvVarCredentialProvider(API_KEY_ENV_VAR);

    try (final AuthClient authClient = AuthClient.create(credentialProvider)) {
      generateTokenWithEnumeratedPermissions(authClient);
      generateTokenWithPredefinedScope(authClient);
    }
    printEndBanner();
  }

  private static void generateTokenWithEnumeratedPermissions(AuthClient authClient)
      throws Exception {
    List<DisposableTokenPermission> permissions = new ArrayList<>();
    permissions.add(
        new DisposableToken.CacheItemPermission(
            CacheRole.ReadWrite, CacheSelector.ByName("cache"), CacheItemSelector.AllCacheItems));
    permissions.add(
        new DisposableToken.CachePermission(CacheRole.ReadOnly, CacheSelector.ByName("topsecret")));
    permissions.add(
        new DisposableToken.TopicPermission(
            TopicRole.PublishSubscribe,
            CacheSelector.ByName("cache"),
            TopicSelector.ByName("topic")));
    DisposableTokenScope scope = new DisposableTokenScope(permissions);
    GenerateDisposableTokenResponse response =
        authClient.generateDisposableTokenAsync(scope, ExpiresIn.minutes(30)).join();
    processResponse(response);
  }

  private static void generateTokenWithPredefinedScope(AuthClient authClient) throws Exception {
    GenerateDisposableTokenResponse response =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadWrite("cache", "cache-key"),
                ExpiresIn.minutes(30))
            .join();
    processResponse(response);
  }

  private static void processResponse(GenerateDisposableTokenResponse response) throws Exception {
    if (response instanceof GenerateDisposableTokenResponse.Success success) {
      printTokenInfo(success);
    } else if (response instanceof GenerateDisposableTokenResponse.Error error) {
      System.out.println(
          "Unable to generate disposable token with error "
              + error.getErrorCode()
              + " "
              + error.getMessage());
      throw new Exception(error.getMessage());
    }
  }

  private static void printTokenInfo(GenerateDisposableTokenResponse.Success token) {
    System.out.println(
        "The generated disposable token (truncated): "
            + token.authToken().substring(0, 10)
            + "..."
            + token.authToken().substring(token.authToken().length() - 10));
    System.out.println(
        "The token expires at (epoch timestamp): " + token.expiresAt().getEpoch() + "\n");
  }

  private static void printStartBanner() {
    System.out.println("******************************************************************");
    System.out.println("Disposable Token Example Start");
    System.out.println("******************************************************************");
  }

  private static void printEndBanner() {
    System.out.println("******************************************************************");
    System.out.println("Disposable Token Example End");
    System.out.println("******************************************************************");
  }
}
