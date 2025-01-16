package momento.sdk;

import java.time.Duration;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.token.TokenGrpc;

/**
 * Manager responsible for GRPC channels and stubs for the Disposable Token.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsTokenGrpcStubsManager extends AbstractGrpcStubsManager<TokenGrpc.TokenFutureStub> {

  private static final Duration DEADLINE = Duration.ofMinutes(1);

  ScsTokenGrpcStubsManager(@Nonnull CredentialProvider credentialProvider) {
    super(
        AbstractGrpcStubsManager.Config.create(
            "auth",
            credentialProvider.getTokenEndpoint(),
            credentialProvider.getAuthToken(),
            // Note: This is hard-coded for now but we may want to expose it via configuration
            // object in the future, as we do with some of the other clients.
            new GrpcConfiguration(DEADLINE),
            TokenGrpc::newFutureStub));
  }
}
