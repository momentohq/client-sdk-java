package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;

abstract class ScsClientBase extends ClientBase {
  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);

  protected Metadata metadataWithCache(String cacheName) {
    return metadataWithItem(CACHE_NAME_KEY, cacheName);
  }
}
