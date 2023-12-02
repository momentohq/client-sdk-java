package momento.sdk;

import java.util.concurrent.CompletableFuture;
import momento.sdk.auth.accessControl.DisposableTokenScope;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.responses.GenerateDisposableTokenResponse;

public class AuthClient implements IAuthClient {
  private final TokenClient tokenClient;

  public AuthClient(TokenClient tokenClient) {
    this.tokenClient = tokenClient;
  }

  @Override
  public CompletableFuture<GenerateDisposableTokenResponse> generateDisposableTokenAsync(
      DisposableTokenScope scope, ExpiresIn expiresIn, String tokenId) {
    try {
      ValidationUtils.checkValidDisposableTokenExpiry(expiresIn);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new GenerateDisposableTokenResponse.Error(new InvalidArgumentException(e.getMessage())));
    }
    return tokenClient.generateDisposableToken(scope, expiresIn, tokenId);
  }

  @Override
  public CompletableFuture<GenerateDisposableTokenResponse> generateDisposableTokenAsync(
      DisposableTokenScope scope, ExpiresIn expiresIn) {
    try {
      ValidationUtils.checkValidDisposableTokenExpiry(expiresIn);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new GenerateDisposableTokenResponse.Error(new InvalidArgumentException(e.getMessage())));
    }
    return tokenClient.generateDisposableToken(scope, expiresIn);
  }

  @Override
  public void close() throws Exception {
    tokenClient.close();
  }
}
