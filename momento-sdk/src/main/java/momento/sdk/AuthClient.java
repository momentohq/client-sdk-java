package momento.sdk;

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.accessControl.DisposableTokenScope;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.responses.auth.GenerateDisposableTokenResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthClient implements IAuthClient {
  private final ScsTokenClient tokenClient;
  private final Logger logger = LoggerFactory.getLogger(AuthClient.class);

  /**
   * Constructs a AuthClient.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   */
  public AuthClient(@Nonnull CredentialProvider credentialProvider) {
    this.tokenClient = new ScsTokenClient(credentialProvider);

    logger.debug("Creating Momento Auth Client");
    logger.debug("Cache endpoint: " + credentialProvider.getCacheEndpoint());
    logger.debug("Control endpoint: " + credentialProvider.getControlEndpoint());
    logger.debug("Token endpoint: " + credentialProvider.getTokenEndpoint());
  }

  /**
   * Constructs a AuthClient.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @return AuthClient
   */
  public static AuthClient create(@Nonnull CredentialProvider credentialProvider) {
    return create(credentialProvider);
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
  public void close() {
    tokenClient.close();
  }
}
