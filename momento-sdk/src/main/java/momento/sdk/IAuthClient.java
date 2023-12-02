package momento.sdk;

import java.util.concurrent.CompletableFuture;
import momento.sdk.auth.accessControl.DisposableTokenScope;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.responses.GenerateDisposableTokenResponse;

public interface IAuthClient extends AutoCloseable {

  CompletableFuture<GenerateDisposableTokenResponse> generateDisposableTokenAsync(
      DisposableTokenScope scope, ExpiresIn expiresIn, String tokenId);

  CompletableFuture<GenerateDisposableTokenResponse> generateDisposableTokenAsync(
      DisposableTokenScope scope, ExpiresIn expiresIn);
}
