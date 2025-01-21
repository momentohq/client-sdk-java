package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.transport.IGrpcConfiguration;

/**
 * Manager responsible for GRPC channels and stubs for the Topics.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsTopicGrpcStubsManager extends AbstractGrpcStubsManager<PubsubGrpc.PubsubStub> {

  ScsTopicGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull IGrpcConfiguration grpcConfiguration) {
    super(
        AbstractGrpcStubsManager.Config.create(
            "topic",
            credentialProvider.getCacheEndpoint(),
            credentialProvider.getAuthToken(),
            grpcConfiguration,
            PubsubGrpc::newStub));
  }
}
