package momento.sdk.storage;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import momento.sdk.BaseStorageTestClass;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.StoreNotFoundException;
import momento.sdk.responses.storage.DeleteResponse;
import momento.sdk.responses.storage.GetResponse;
import momento.sdk.responses.storage.PutResponse;
import org.junit.jupiter.api.Test;

public class DataTests extends BaseStorageTestClass {
  @Test
  void getReturnsValueAsStringAfterPut() {
    final String key = randomString("key");
    final String value = randomString("value");

    // Successful Set
    final PutResponse putResponse = storageClient.put(storeName, key, value).join();
    assertThat(putResponse).isInstanceOf(PutResponse.Success.class);

    // Successful Get
    final GetResponse getResponse = storageClient.get(storeName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.valueWhenFound().get().getString().get()).isEqualTo(value);
  }

  @Test
  void getReturnsValueAsByteArrayAfterPut() {
    final String key = randomString("key");
    final String value = randomString("value");

    // Successful Set
    final PutResponse putResponse = storageClient.put(storeName, key, value.getBytes()).join();
    assertThat(putResponse).isInstanceOf(PutResponse.Success.class);

    // Successful Get
    final GetResponse getResponse = storageClient.get(storeName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.valueWhenFound().get().getByteArray().get()).isEqualTo(value.getBytes());
  }

  @Test
  void getReturnsValueAsLongAfterPut() {
    final String key = randomString("key");
    final long value = 42L;

    // Successful Set
    final PutResponse putResponse = storageClient.put(storeName, key, value).join();
    assertThat(putResponse).isInstanceOf(PutResponse.Success.class);

    // Successful Get
    final GetResponse getResponse = storageClient.get(storeName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.valueWhenFound().get().getLong().get()).isEqualTo(value);
  }

  @Test
  void getReturnsValueAsDoubleAfterPut() {
    final String key = randomString("key");
    final double value = 3.14;

    // Successful Set
    final PutResponse putResponse = storageClient.put(storeName, key, value).join();
    assertThat(putResponse).isInstanceOf(PutResponse.Success.class);

    // Successful Get
    final GetResponse getResponse = storageClient.get(storeName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.valueWhenFound().get().getDouble().get()).isEqualTo(value);
  }

  @Test
  void storeKeyNotFound() {
    // Get key that was not set
    final GetResponse response = storageClient.get(storeName, randomString("key")).join();
    assertThat(response).isInstanceOf(GetResponse.NotFound.class);
    assert response.valueWhenFound().isEmpty();
    assert response instanceof GetResponse.NotFound;
    assertThrows(ClientSdkException.class, response.valueWhenFound()::orElseThrow);
  }

  @Test
  public void badStoreNameReturnsError() {
    final String storeName = randomString("name");

    final GetResponse getResponse = storageClient.get(storeName, "").join();
    assertThat(getResponse).isInstanceOf(GetResponse.Error.class);
    assertThat(((GetResponse.Error) getResponse)).hasCauseInstanceOf(StoreNotFoundException.class);

    final PutResponse putResponse = storageClient.put(storeName, "", "").join();
    assertThat(putResponse).isInstanceOf(PutResponse.Error.class);
    assertThat(((PutResponse.Error) putResponse)).hasCauseInstanceOf(StoreNotFoundException.class);
  }

  @Test
  public void allowEmptyKeyValuesOnGet() throws Exception {
    final String emptyKey = "";
    final String emptyValue = "";
    storageClient.put(storeName, emptyKey, emptyValue).get();
    final GetResponse response = storageClient.get(storeName, emptyKey).get();
    assertThat(response).isInstanceOf(GetResponse.Found.class);
    assert response.valueWhenFound().get().getString().get().isEmpty();
  }

  @Test
  public void deleteHappyPath() throws Exception {
    final String key = "key";
    final String value = "value";

    storageClient.put(storeName, key, value).get();
    final GetResponse getResponse = storageClient.get(storeName, key).get();
    assertThat(getResponse).isInstanceOf(GetResponse.Found.class);
    assertThat(getResponse.valueWhenFound().get().getString().get()).isEqualTo(value);

    final DeleteResponse deleteResponse = storageClient.delete(storeName, key).get();
    assertThat(deleteResponse).isInstanceOf(DeleteResponse.Success.class);

    final GetResponse getAfterDeleteResponse = storageClient.get(storeName, key).get();
    assert getAfterDeleteResponse.valueWhenFound().isEmpty();
    assert getAfterDeleteResponse instanceof GetResponse.NotFound;
  }

  @Test
  public void deleteNonExistentKey() throws Exception {
    final String key = randomString("key");

    final DeleteResponse deleteResponse = storageClient.delete(storeName, key).get();
    assertThat(deleteResponse).isInstanceOf(DeleteResponse.Success.class);
  }
}
