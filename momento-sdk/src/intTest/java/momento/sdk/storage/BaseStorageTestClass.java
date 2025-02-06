package momento.sdk.storage;

import java.time.Duration;
import java.util.UUID;
import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfigurations;
import momento.sdk.responses.storage.CreateStoreResponse;
import momento.sdk.responses.storage.DeleteStoreResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseStorageTestClass {
  protected static final Duration THIRTY_SECONDS = Duration.ofSeconds(30);
  protected static CredentialProvider credentialProvider;
  protected static PreviewStorageClient storageClient;
  protected static String storeName;

  @BeforeAll
  static void beforeAll() {
    credentialProvider = CredentialProvider.fromEnvVar("MOMENTO_API_KEY");
    storageClient =
        new PreviewStorageClient(credentialProvider, StorageConfigurations.Laptop.latest());
    storeName = testStoreName();
    ensureTestStoreExists(storeName);
  }

  @AfterAll
  static void afterAll() {
    cleanupTestStore(storeName);
    storageClient.close();
  }

  protected static void ensureTestStoreExists(String storeName) {
    CreateStoreResponse response = storageClient.createStore(storeName).join();
    if (response instanceof CreateStoreResponse.Error) {
      throw new RuntimeException(
          "Failed to test create store: " + ((CreateStoreResponse.Error) response).getMessage());
    }
  }

  public static void cleanupTestStore(String storeName) {
    DeleteStoreResponse response = storageClient.deleteStore(storeName).join();
    if (response instanceof DeleteStoreResponse.Error) {
      throw new RuntimeException(
          "Failed to test delete store: " + ((DeleteStoreResponse.Error) response).getMessage());
    }
  }

  public static String testStoreName() {
    return "java-integration-test-default-" + UUID.randomUUID();
  }
}
