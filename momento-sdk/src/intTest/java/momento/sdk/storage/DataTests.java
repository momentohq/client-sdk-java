package momento.sdk.storage;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import momento.sdk.BaseTestClass;
import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfigurations;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.StoreNotFoundException;
import momento.sdk.responses.storage.DeleteResponse;
import momento.sdk.responses.storage.GetResponse;
import momento.sdk.responses.storage.PutResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DataTests extends BaseTestClass {
  private static PreviewStorageClient client;

  // TODO can set to the same value as the cache tests
  // TODO rename env var for clarity to TEST_RESOURCE_NAME or similar
  private final String storeName = System.getenv("TEST_CACHE_NAME");

  @BeforeAll
  static void setup() {
    client =
        new PreviewStorageClient(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            StorageConfigurations.Laptop.latest());

    client.createStore(System.getenv("TEST_CACHE_NAME")).join();
  }

  @AfterAll
  static void tearDown() {
    client.close();
  }

  @Test
  void getReturnsValueAsStringAfterPut() {
    final String key = randomString("key");
    final String value = randomString("value");

    // Successful Set
    final PutResponse putResponse = client.put(storeName, key, value).join();
    assertThat(putResponse).isInstanceOf(PutResponse.Success.class);

    // Successful Get
    final GetResponse getResponse = client.get(storeName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.found().get().value().getString().get()).isEqualTo(value);
  }

  @Test
  void getReturnsValueAsByteArrayAfterPut() {
    final String key = randomString("key");
    final String value = randomString("value");

    // Successful Set
    final PutResponse putResponse = client.put(storeName, key, value.getBytes()).join();
    assertThat(putResponse).isInstanceOf(PutResponse.Success.class);

    // Successful Get
    final GetResponse getResponse = client.get(storeName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.found().get().value().getByteArray().get()).isEqualTo(value.getBytes());
  }

  @Test
  void getReturnsValueAsLongAfterPut() {
    final String key = randomString("key");
    final long value = 42L;

    // Successful Set
    final PutResponse putResponse = client.put(storeName, key, value).join();
    assertThat(putResponse).isInstanceOf(PutResponse.Success.class);

    // Successful Get
    final GetResponse getResponse = client.get(storeName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.found().get().value().getLong().get()).isEqualTo(value);
  }

  @Test
  void getReturnsValueAsDoubleAfterPut() {
    final String key = randomString("key");
    final double value = 3.14;

    // Successful Set
    final PutResponse putResponse = client.put(storeName, key, value).join();
    assertThat(putResponse).isInstanceOf(PutResponse.Success.class);

    // Successful Get
    final GetResponse getResponse = client.get(storeName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.found().get().value().getDouble().get()).isEqualTo(value);
  }

  @Test
  void storeKeyNotFound() {
    // Get key that was not set
    final GetResponse response = client.get(storeName, randomString("key")).join();
    assertThat(response).isInstanceOf(GetResponse.NotFound.class);
    assert response.found().isEmpty();
    assert response instanceof GetResponse.NotFound;
    assertThrows(ClientSdkException.class, response.found()::orElseThrow);
  }

  @Test
  public void badStoreNameReturnsError() {
    final String storeName = randomString("name");

    final GetResponse getResponse = client.get(storeName, "").join();
    assertThat(getResponse).isInstanceOf(GetResponse.Error.class);
    assertThat(((GetResponse.Error) getResponse)).hasCauseInstanceOf(StoreNotFoundException.class);

    final PutResponse putResponse = client.put(storeName, "", "").join();
    assertThat(putResponse).isInstanceOf(PutResponse.Error.class);
    assertThat(((PutResponse.Error) putResponse)).hasCauseInstanceOf(StoreNotFoundException.class);
  }

  @Test
  public void allowEmptyKeyValuesOnGet() throws Exception {
    final String emptyKey = "";
    final String emptyValue = "";
    client.put(storeName, emptyKey, emptyValue).get();
    final GetResponse response = client.get(storeName, emptyKey).get();
    assertThat(response).isInstanceOf(GetResponse.Found.class);
    assert response.found().get().value().getString().get().isEmpty();
  }

  @Test
  public void deleteHappyPath() throws Exception {
    final String key = "key";
    final String value = "value";

    client.put(storeName, key, value).get();
    final GetResponse getResponse = client.get(storeName, key).get();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.found().get().value().getString().get()).isEqualTo(value);

    final DeleteResponse deleteResponse = client.delete(storeName, key).get();
    assertThat(deleteResponse).isInstanceOf(DeleteResponse.Success.class);

    final GetResponse getAfterDeleteResponse = client.get(storeName, key).get();
    assert getAfterDeleteResponse.found().isEmpty();
    assert getAfterDeleteResponse instanceof GetResponse.NotFound;
  }

  @Test
  public void deleteNonExistentKey() throws Exception {
    final String key = randomString("key");

    final DeleteResponse deleteResponse = client.delete(storeName, key).get();
    assertThat(deleteResponse).isInstanceOf(DeleteResponse.Success.class);
  }
}
