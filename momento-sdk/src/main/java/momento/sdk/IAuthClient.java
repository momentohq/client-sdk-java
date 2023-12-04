package momento.sdk;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import momento.sdk.auth.accessControl.DisposableTokenScope;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.responses.auth.GenerateDisposableTokenResponse;

public interface IAuthClient extends Closeable {

  CompletableFuture<GenerateDisposableTokenResponse> generateDisposableTokenAsync(
      DisposableTokenScope scope, ExpiresIn expiresIn, String tokenId);

  CompletableFuture<GenerateDisposableTokenResponse> generateDisposableTokenAsync(
      DisposableTokenScope scope, ExpiresIn expiresIn);
}
