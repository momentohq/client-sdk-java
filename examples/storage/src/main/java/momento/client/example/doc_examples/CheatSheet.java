package momento.client.example.doc_examples;

import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfigurations;

public class CheatSheet {
  public static void main(String[] args) {
    try (final var storageClient =
                 new PreviewStorageClient(
                         CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
                         StorageConfigurations.Laptop.latest())) {
      // ...
    }
  }
}
