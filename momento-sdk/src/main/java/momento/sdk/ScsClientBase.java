package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

abstract class ScsClientBase extends ClientBase {
  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> SDK_AGENT_KEY =
          Metadata.Key.of("Agent", ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> RUNTIME_VERSION_KEY =
          Metadata.Key.of("Runtime-Version", ASCII_STRING_MARSHALLER);
  private boolean hasSentOneTimeHeaders = false;

  protected Metadata metadataWithCache(String cacheName) {
    if (hasSentOneTimeHeaders) {
      Metadata metadata = new Metadata();
      metadata.put(CACHE_NAME_KEY, cacheName);
      return metadata;
    }
    hasSentOneTimeHeaders = true;
    String sdkVersion =
            String.format("java:cache:%s", this.getClass().getPackage().getImplementationVersion());
    String runtimeVer =
            System.getProperty("java.vendor") + ", " + System.getProperty("java.version");

    Metadata metadata = new Metadata();
    metadata.put(CACHE_NAME_KEY, cacheName);
    metadata.put(SDK_AGENT_KEY, sdkVersion);
    metadata.put(RUNTIME_VERSION_KEY, runtimeVer);
    return metadata;
  }

  protected <S extends AbstractStub<S>> S attachObservableMetadata(S stub, Metadata metadata) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }
}
