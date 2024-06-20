package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

abstract class ScsClientBase extends ClientBase {
  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);

  protected Metadata metadataWithCache(String cacheName) {
    return metadataWithItem(CACHE_NAME_KEY, cacheName);
  }

  protected <S extends AbstractStub<S>> S attachObservableMetadata(S stub, Metadata metadata) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }
}
