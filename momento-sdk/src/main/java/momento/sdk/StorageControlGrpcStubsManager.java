package momento.sdk;

import grpc.control_client.ScsControlGrpc;
import java.time.Duration;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.transport.IGrpcConfiguration;

/**
 * Manager responsible for GRPC channels and stubs for the Control Plane.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class StorageControlGrpcStubsManager
    extends AbstractGrpcStubsManager<ScsControlGrpc.ScsControlFutureStub> {

  private static final Duration DEADLINE = Duration.ofMinutes(1);

  StorageControlGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull IGrpcConfiguration grpcConfiguration) {
    super(
        AbstractGrpcStubsManager.Config.create(
            "store",
            credentialProvider.getControlEndpoint(),
            credentialProvider.getAuthToken(),
            grpcConfiguration.withDeadline(DEADLINE),
            ScsControlGrpc::newFutureStub));
  }
}
