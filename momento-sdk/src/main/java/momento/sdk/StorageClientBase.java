package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;
import javax.annotation.Nullable;

abstract class StorageClientBase extends ClientBase {
  private static final Metadata.Key<String> STORE_NAME_KEY =
      Metadata.Key.of("store", ASCII_STRING_MARSHALLER);

  public StorageClientBase(@Nullable Integer concurrencyLimit) {
    super(concurrencyLimit);
  }

  protected Metadata metadataWithStore(String storeName) {
    return metadataWithItem(STORE_NAME_KEY, storeName);
  }
}
