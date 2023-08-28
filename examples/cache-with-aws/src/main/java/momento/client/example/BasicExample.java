package momento.client.example;

import java.time.Duration;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheInfo;
import momento.sdk.responses.cache.control.CacheListResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class BasicExample {

  private static final String AUTH_TOKEN_SECRET_NAME = "MOMENTO_AUTH_TOKEN";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  private static final Logger logger = LoggerFactory.getLogger(BasicExample.class);
  private static final String CACHE_NAME = "cache";
  private static final String KEY = "key";
  private static final String VALUE = "value";

  public static void main(String[] args) {
    printStartBanner();

    final CredentialProvider credentialProvider = getCredentialsFromSecretsManagerAuthToken();

    try (final CacheClient client =
        CacheClient.create(
            credentialProvider, Configurations.Laptop.latest(), DEFAULT_ITEM_TTL, null)) {

      createCache(client, CACHE_NAME);

      listCaches(client);

      System.out.printf("Setting key '%s', value '%s'%n", KEY, VALUE);
      client.set(CACHE_NAME, KEY, VALUE).join();

      System.out.printf("Getting value for key '%s'%n", KEY);

      final GetResponse getResponse = client.get(CACHE_NAME, KEY).join();
      if (getResponse instanceof GetResponse.Hit hit) {
        System.out.printf("Found value for key '%s': '%s'%n", KEY, hit.valueString());
      } else if (getResponse instanceof GetResponse.Miss) {
        System.out.println("Found no value for key " + KEY);
      } else if (getResponse instanceof GetResponse.Error error) {
        System.out.printf(
            "Unable to look up value for key '%s' with error %s\n", KEY, error.getErrorCode());
        System.out.println(error.getMessage());
      }
    }
    printEndBanner();
  }

  private static void createCache(CacheClient cacheClient, String cacheName) {
    final CacheCreateResponse createResponse = cacheClient.createCache(cacheName).join();
    if (createResponse instanceof CacheCreateResponse.Error error) {
      if (error.getCause() instanceof AlreadyExistsException) {
        System.out.println("Cache with name '" + cacheName + "' already exists.");
      } else {
        System.out.println("Unable to create cache with error " + error.getErrorCode());
        System.out.println(error.getMessage());
      }
    }
  }

  private static void listCaches(CacheClient cacheClient) {
    System.out.println("Listing caches:");
    final CacheListResponse listResponse = cacheClient.listCaches().join();
    if (listResponse instanceof CacheListResponse.Success success) {
      for (CacheInfo cacheInfo : success.getCaches()) {
        System.out.println(cacheInfo.name());
      }
    } else if (listResponse instanceof CacheListResponse.Error error) {
      System.out.println("Unable to list caches with error " + error.getErrorCode());
      System.out.println(error.getMessage());
    }
  }

  public static CredentialProvider getCredentialsFromSecretsManagerAuthToken() {
    final Region region = Region.of("us-east-1");

    // Create a Secrets Manager client
    final SecretsManagerClient client =
        SecretsManagerClient.builder()
            .region(region)
            // make sure to configure aws credentials in order to use the default provider
            // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    final GetSecretValueRequest getSecretValueRequest =
        GetSecretValueRequest.builder().secretId(AUTH_TOKEN_SECRET_NAME).build();

    final GetSecretValueResponse getSecretValueResponse;

    try {
      getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
    } catch (Exception e) {
      // For a list of exceptions thrown, see
      // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
      throw e;
    }

    final String secret = getSecretValueResponse.secretString();
    try {
      return CredentialProvider.fromString(secret);
    } catch (SdkException e) {
      logger.error(
          "An error occured while parsing the secrets manager vended"
              + " authentication token , key: "
              + AUTH_TOKEN_SECRET_NAME,
          e);
      throw e;
    }
  }

  private static void printStartBanner() {
    System.out.println("******************************************************************");
    System.out.println("Basic Example Start");
    System.out.println("******************************************************************");
  }

  private static void printEndBanner() {
    System.out.println("******************************************************************");
    System.out.println("Basic Example End");
    System.out.println("******************************************************************");
  }
}
