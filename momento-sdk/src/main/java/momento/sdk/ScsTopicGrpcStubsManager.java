package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import java.io.Closeable;
import java.util.UUID;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfiguration;

/**
 * Manager responsible for GRPC channels and stubs for the Topics.
 *
 * <p>The business layer, will get request stubs from this layer. This keeps the two layers
 * independent and any future pooling of channels can happen exclusively in the manager without
 * impacting the API business logic.
 */
final class ScsTopicGrpcStubsManager implements Closeable {
  public static final UUID CONNECTION_ID_KEY = UUID.randomUUID();

  private final UnaryTopicGrpcConnectionPool unaryConnectionPool;
  private final StreamTopicGrpcConnectionPool streamConnectionPool;

  private final TopicConfiguration configuration;

  ScsTopicGrpcStubsManager(
      @Nonnull CredentialProvider credentialProvider, @Nonnull TopicConfiguration configuration) {
    this.configuration = configuration;
    this.unaryConnectionPool =
        new StaticUnaryGrpcConnectionPool(credentialProvider, configuration, CONNECTION_ID_KEY);

    if (configuration.getIsNumStreamChannelsDynamic()) {
      this.streamConnectionPool =
          new DynamicStreamGrpcConnectionPool(credentialProvider, configuration, CONNECTION_ID_KEY);
    } else {
      this.streamConnectionPool =
          new StaticStreamGrpcConnectionPool(credentialProvider, configuration, CONNECTION_ID_KEY);
    }
  }

  TopicConfiguration getConfiguration() {
    return configuration;
  }

  StreamStubWithCount getNextStreamStub() {
    return streamConnectionPool.getNextStreamStub();
  }

  PubsubGrpc.PubsubStub getNextUnaryStub() {
    return unaryConnectionPool.getNextUnaryStub();
  }

  @Override
  public void close() {
    unaryConnectionPool.close();
    streamConnectionPool.close();
  }
}
