package momento.sdk;

import grpc.cache_client.ScsGrpc;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;

/**
 * Manager responsible for GRPC channels and stubs for the Data Plane.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsDataGrpcStubsManager extends AbstractGrpcStubsManager<ScsGrpc.ScsFutureStub> {

  private final List<ScsGrpc.ScsStub> observableStubs;

  ScsDataGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider, @Nonnull Configuration configuration) {
    super(
        AbstractGrpcStubsManager.Config.create(
                "cache",
                credentialProvider.getCacheEndpoint(),
                credentialProvider.getAuthToken(),
                configuration.getTransportStrategy().getGrpcConfiguration(),
                ScsGrpc::newFutureStub)
            .withReadConcern(configuration.getReadConcern())
            .withRetryStrategy(configuration.getRetryStrategy()));

    this.observableStubs = channels.stream().map(ScsGrpc::newStub).collect(Collectors.toList());
  }

  /**
   * Returns a stream observable stub with appropriate deadlines.
   *
   * <p>Each stub is deliberately decorated with Deadline. Deadlines work differently than timeouts.
   * When a deadline is set on a stub, it simply means that once the stub is created it must be used
   * before the deadline expires. Hence, the stub returned from here should never be cached and the
   * safest behavior is for clients to request a new stub each time.
   *
   * <p><a href="https://github.com/grpc/grpc-java/issues/1495">more information</a>
   */
  ScsGrpc.ScsStub getObservableStub() {
    return getNextStub(observableStubs);
  }
}
