package momento.sdk;

import grpc.store.StoreGrpc;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.transport.IGrpcConfiguration;

/**
 * Manager responsible for GRPC channels and stubs for the Data Plane.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class StorageDataGrpcStubsManager
    extends AbstractGrpcStubsManager<StoreGrpc.StoreFutureStub> {

  StorageDataGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull IGrpcConfiguration grpcConfiguration) {
    super(
        AbstractGrpcStubsManager.Config.create(
            "store",
            credentialProvider.getCacheEndpoint(),
            credentialProvider.getAuthToken(),
            grpcConfiguration,
            StoreGrpc::newFutureStub));
  }
}
