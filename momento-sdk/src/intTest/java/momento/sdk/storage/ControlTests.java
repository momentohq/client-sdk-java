package momento.sdk.storage;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import momento.sdk.BaseTestClass;
import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfigurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.BadRequestException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.responses.storage.control.CreateStoreResponse;
import momento.sdk.responses.storage.control.DeleteStoreResponse;
import momento.sdk.responses.storage.control.ListStoresResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ControlTests extends BaseTestClass {
  private static PreviewStorageClient client;

  public static final Duration TEN_SECONDS = Duration.ofSeconds(10);

  @BeforeAll
  static void setup() {
    client =
        new PreviewStorageClient(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            StorageConfigurations.Laptop.latest());

    // delete all stores
    ListStoresResponse response = client.listStores().join();
    ((ListStoresResponse.Success) response)
        .getStores()
        .forEach(store -> client.deleteStore(store.getName()).join());

    // TODO re-using this name
    client.createStore(System.getenv("TEST_CACHE_NAME")).join();
  }

  @AfterAll
  static void tearDown() {
    client.close();
  }

  @Test
  public void returnsAlreadyExistsWhenCreatingExistingStore() {
    // TODO externalize this
    // TODO rename env var to something broader like TEST_RESOURCE_NAME
    final String existingStore = System.getenv("TEST_CACHE_NAME");

    assertThat(client.createStore(existingStore))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.AlreadyExists.class));
  }

  @Test
  public void returnsNotFoundWhenDeletingUnknownStore() {
    assertThat(client.deleteStore(randomString("name")))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void listsStoresHappyPath() {
    final String storeName = randomString("name");

    assertThat(client.createStore(storeName))
        .succeedsWithin(TEN_SECONDS)
        .isInstanceOf(CreateStoreResponse.Success.class);

    try {
      assertThat(client.listStores())
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(ListStoresResponse.Success.class))
          .satisfies(
              success ->
                  assertThat(success.getStores())
                      .anyMatch(storeInfo -> storeInfo.getName().equals(storeName)));
    } finally {
      // cleanup
      assertThat(client.deleteStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .isInstanceOf(DeleteStoreResponse.Success.class);
    }
  }

  @Test
  public void returnsBadRequestForEmptyStoreName() {
    assertThat(client.createStore("      "))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(BadRequestException.class));
  }

  @Test
  public void throwsValidationExceptionForNullStoreName() {
    assertThat(client.createStore(null))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(client.deleteStore(null))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void deleteSucceeds() {
    final String storeName = randomString("name");

    assertThat(client.createStore(storeName))
        .succeedsWithin(TEN_SECONDS)
        .isInstanceOf(CreateStoreResponse.Success.class);

    assertThat(client.createStore(storeName))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(AlreadyExistsException.class));

    assertThat(client.deleteStore(storeName))
        .succeedsWithin(TEN_SECONDS)
        .isInstanceOf(DeleteStoreResponse.Success.class);

    assertThat(client.deleteStore(storeName))
        .succeedsWithin(TEN_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void returnsErrorForBadToken() {
    final String storeName = randomString("name");
    final String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b"
            + "2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmml"
            + "s76573jnajhjjjhjdhnndy";
    final CredentialProvider badTokenProvider = CredentialProvider.fromString(badToken);

    try (final PreviewStorageClient client =
        new PreviewStorageClient(
            CredentialProvider.fromString(badToken),
            StorageConfigurations.Laptop.latest()) /*CacheClient.builder(
                                 badTokenProvider, Configurations.Laptop.latest(), Duration.ofSeconds(10))
                         .build()*/) {
      assertThat(client.createStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(client.deleteStore(storeName))
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(client.listStores())
          .succeedsWithin(TEN_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(ListStoresResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));
    }
  }
}
