package momento.sdk;

import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;

/** Builder for {@link AuthClient} */
public final class AuthClientBuilder {

  private final CredentialProvider credentialProvider;

  /**
   * Creates a AuthClient builder.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   */
  AuthClientBuilder(@Nonnull CredentialProvider credentialProvider) {
    this.credentialProvider = credentialProvider;
  }

  /**
   * Builds a AuthClient.
   *
   * @return the client.
   */
  public AuthClient build() {
    return new AuthClient(credentialProvider);
  }
}
