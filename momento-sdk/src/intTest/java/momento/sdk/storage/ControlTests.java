package momento.sdk.storage;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import momento.sdk.BaseTestClass;
import momento.sdk.IPreviewStorageClient;
import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.BadRequestException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.responses.storage.control.CreateStoreResponse;
import momento.sdk.responses.storage.control.DeleteStoreResponse;
import momento.sdk.responses.storage.control.ListStoresResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ControlTests extends BaseTestClass {
  private IPreviewStorageClient target;

  @BeforeEach
  void setup() {
    /*target =
    CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
            .build();*/
    target = new PreviewStorageClient();
  }

  @AfterEach
  void tearDown() {
    /*target.close();*/
  }

  @Test
  public void throwsAlreadyExistsWhenCreatingExistingStore() {
    final String existingStore = System.getenv("TEST_STORE_NAME");

    assertThat(target.createStore(existingStore))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(AlreadyExistsException.class));
  }

  @Test
  public void returnsNotFoundWhenDeletingUnknownStore() {
    assertThat(target.deleteStore(randomString("name")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void listsStoresHappyPath() {
    final String storeName = randomString("name");

    assertThat(target.createStore(storeName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CreateStoreResponse.Success.class);

    try {
      assertThat(target.listStores())
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(ListStoresResponse.Success.class))
          .satisfies(
              success ->
                  assertThat(success.getStores())
                      .anyMatch(storeInfo -> storeInfo.getName().equals(storeName)));
    } finally {
      // cleanup
      assertThat(target.deleteStore(storeName))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(DeleteStoreResponse.Success.class);
    }
  }

  @Test
  public void returnsBadRequestForEmptyStoreName() {
    assertThat(target.createStore("      "))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(BadRequestException.class));
  }

  @Test
  public void throwsValidationExceptionForNullStoreName() {
    assertThat(target.createStore(null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(target.deleteStore(null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void deleteSucceeds() {
    final String storeName = randomString("name");

    assertThat(target.createStore(storeName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CreateStoreResponse.Success.class);

    assertThat(target.createStore(storeName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(AlreadyExistsException.class));

    assertThat(target.deleteStore(storeName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DeleteStoreResponse.Success.class);

    assertThat(target.deleteStore(storeName))
        .succeedsWithin(FIVE_SECONDS)
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

    try (final PreviewStorageClient client = new PreviewStorageClient() /*CacheClient.builder(
                                 badTokenProvider, Configurations.Laptop.latest(), Duration.ofSeconds(10))
                         .build()*/) {
      assertThat(client.createStore(storeName))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CreateStoreResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(client.deleteStore(storeName))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(DeleteStoreResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(client.listStores())
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(ListStoresResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));
    }
  }
}
