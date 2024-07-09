package momento.sdk.storage;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import momento.sdk.BaseStorageTestClass;
import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfigurations;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.BadRequestException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.StoreNotFoundException;
import momento.sdk.responses.storage.CreateStoreResponse;
import momento.sdk.responses.storage.DeleteStoreResponse;
import momento.sdk.responses.storage.ListStoresResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class ControlTests extends BaseStorageTestClass {
  @Test
  public void returnsAlreadyExistsWhenCreatingExistingStore() {
    // TODO externalize this
    // TODO rename env var to something broader like TEST_RESOURCE_NAME
    final String existingStore = System.getenv("TEST_CACHE_NAME");

    assertThat(storageClient.createStore(existingStore))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.AlreadyExists.class));
  }

  @Test
  public void returnsNotFoundWhenDeletingUnknownStore() {
    assertThat(storageClient.deleteStore(randomString("name")))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(StoreNotFoundException.class));
  }

  @Test
  public void listsStoresHappyPath() {
    final String storeName = randomString("name");

    assertThat(storageClient.createStore(storeName))
        .succeedsWithin(TEN_SECONDS)
        .isInstanceOf(CreateStoreResponse.Success.class);

    try {
      assertThat(storageClient.listStores())
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(ListStoresResponse.Success.class))
          .satisfies(
              success ->
                  assertThat(success.getStores())
                      .anyMatch(storeInfo -> storeInfo.getName().equals(storeName)));

      final ListStoresResponse response = storageClient.listStores().join();
      assertThat(response).isInstanceOf(ListStoresResponse.Success.class);
    } finally {
      // cleanup
      assertThat(storageClient.deleteStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .isInstanceOf(DeleteStoreResponse.Success.class);
    }
  }

  @Test
  public void returnsBadRequestForEmptyStoreName() {
    assertThat(storageClient.createStore("      "))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(BadRequestException.class));
  }

  @Test
  public void throwsValidationExceptionForNullStoreName() {
    assertThat(storageClient.createStore(null))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(storageClient.deleteStore(null))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void deleteSucceeds() {
    final String storeName = randomString("name");

    try {
      assertThat(storageClient.createStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .isInstanceOf(CreateStoreResponse.Success.class);

      assertThat(storageClient.createStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.AlreadyExists.class));

      assertThat(storageClient.deleteStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .isInstanceOf(DeleteStoreResponse.Success.class);

      assertThat(storageClient.deleteStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(StoreNotFoundException.class));
    } finally {
      // Just in case the second create or delete fails
      storageClient.deleteStore(storeName).join();
    }
  }

  @Test
  public void returnsErrorForBadToken() {
    final String storeName = randomString("name");
    final String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b"
            + "2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmml"
            + "s76573jnajhjjjhjdhnndy";
    final CredentialProvider badTokenProvider = CredentialProvider.fromString(badToken);

    try (final PreviewStorageClient storageClient =
        new PreviewStorageClient(
            CredentialProvider.fromString(badToken),
            StorageConfigurations.Laptop.latest()) /*CacheStorageClient.builder(
                                 badTokenProvider, Configurations.Laptop.latest(), Duration.ofSeconds(10))
                         .build()*/) {
      assertThat(storageClient.createStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(storageClient.deleteStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(storageClient.listStores())
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(ListStoresResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));
    }
  }
}
