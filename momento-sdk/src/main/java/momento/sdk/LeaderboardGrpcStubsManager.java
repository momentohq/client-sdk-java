package momento.sdk;

import grpc.leaderboard.LeaderboardGrpc;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.transport.IGrpcConfiguration;

/** Manager responsible for GRPC channels and stubs for leaderboards. */
final class LeaderboardGrpcStubsManager
    extends AbstractGrpcStubsManager<LeaderboardGrpc.LeaderboardFutureStub> {

  LeaderboardGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull IGrpcConfiguration grpcConfiguration) {
    super(
        AbstractGrpcStubsManager.Config.create(
            "leaderboard",
            credentialProvider.getCacheEndpoint(),
            credentialProvider.getAuthToken(),
            grpcConfiguration,
            LeaderboardGrpc::newFutureStub));
  }
}
