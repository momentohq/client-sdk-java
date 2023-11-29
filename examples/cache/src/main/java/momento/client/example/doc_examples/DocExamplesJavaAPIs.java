package momento.client.example.doc_examples;

import java.time.Duration;
import java.util.stream.Collectors;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.IncrementResponse;
import momento.sdk.responses.cache.SetIfNotExistsResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import momento.sdk.responses.cache.control.CacheFlushResponse;
import momento.sdk.responses.cache.control.CacheInfo;
import momento.sdk.responses.cache.control.CacheListResponse;

public class DocExamplesJavaAPIs {

  public static final String FAKE_V1_API_KEY =
      "eyJhcGlfa2V5IjogImV5SjBlWEFpT2lKS1YxUWlMQ0poYkdjaU9pSklVekkxTmlKOS5l"
          + "eUpwYzNNaU9pSlBibXhwYm1VZ1NsZFVJRUoxYVd4a1pYSWlMQ0pwWVhRaU9qRTJOemd6TURVNE1USXNJbVY0Y0NJNk5EZzJOVFV4TlRReE1pd"
          + "2lZWFZrSWpvaUlpd2ljM1ZpSWpvaWFuSnZZMnRsZEVCbGVHRnRjR3hsTG1OdmJTSjkuOEl5OHE4NExzci1EM1lDb19IUDRkLXhqSGRUOFVDSX"
          + "V2QVljeGhGTXl6OCIsICJlbmRwb2ludCI6ICJ0ZXN0Lm1vbWVudG9ocS5jb20ifQo=";

  public static String retrieveAuthTokenFromYourSecretsManager() {
    return FAKE_V1_API_KEY;
  }

  public static void example_API_CredentialProviderFromEnvVar() {
    CredentialProvider.fromEnvVar("MOMENTO_API_KEY");
  }

  public static void example_API_CredentialProviderFromString() {
    final String authToken = retrieveAuthTokenFromYourSecretsManager();
    CredentialProvider.fromString(authToken);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static void example_API_ConfigurationLaptop() {
    Configurations.Laptop.v1();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static void example_API_ConfigurationInRegionLatest() {
    Configurations.InRegion.latest();
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  public static void example_API_ConfigurationLowLatency() {
    Configurations.LowLatency.latest();
  }

  @SuppressWarnings("EmptyTryBlock")
  public static void example_API_InstantiateCacheClient() {
    try (CacheClient cacheClient =
        CacheClient.create(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            Configurations.Laptop.v1(),
            Duration.ofSeconds(60))) {
      // ...
    }
  }

  public static void example_API_ErrorHandlingHitMiss(CacheClient cacheClient) {
    final GetResponse response = cacheClient.get("test-cache", "test-key").join();
    if (response instanceof GetResponse.Hit hit) {
      System.out.println("Retrieved value for key 'test-key': " + hit.valueString());
    } else if (response instanceof GetResponse.Miss) {
      System.out.println("Key 'test-key' was not found in cache 'test-cache'");
    } else if (response instanceof GetResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to get key 'test-key' from cache 'test-cache': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_ErrorHandlingSuccess(CacheClient cacheClient) {
    final SetResponse response = cacheClient.set("test-cache", "test-key", "test-value").join();
    if (response instanceof SetResponse.Success) {
      System.out.println("Key 'test-key' stored successfully");
    } else if (response instanceof SetResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to store key 'test-key' in cache 'test-cache': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_CreateCache(CacheClient cacheClient) {
    final CacheCreateResponse response = cacheClient.createCache("test-cache").join();
    if (response instanceof CacheCreateResponse.Success) {
      System.out.println("Cache 'test-cache' created");
    } else if (response instanceof CacheCreateResponse.Error error) {
      if (error.getCause() instanceof AlreadyExistsException) {
        System.out.println("Cache 'test-cache' already exists");
      } else {
        throw new RuntimeException(
            "An error occurred while attempting to create cache 'test-cache': "
                + error.getErrorCode(),
            error);
      }
    }
  }

  public static void example_API_DeleteCache(CacheClient cacheClient) {
    final CacheDeleteResponse response = cacheClient.deleteCache("test-cache").join();
    if (response instanceof CacheDeleteResponse.Success) {
      System.out.println("Cache 'test-cache' deleted");
    } else if (response instanceof CacheDeleteResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to delete cache 'test-cache': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_ListCaches(CacheClient cacheClient) {
    final CacheListResponse response = cacheClient.listCaches().join();
    if (response instanceof CacheListResponse.Success success) {
      final String caches =
          success.getCaches().stream().map(CacheInfo::name).collect(Collectors.joining("\n"));
      System.out.println("Caches:\n" + caches);
    } else if (response instanceof CacheListResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to list caches: " + error.getErrorCode(), error);
    }
  }

  public static void example_API_FlushCache(CacheClient cacheClient) {
    final CacheFlushResponse response = cacheClient.flushCache("test-cache").join();
    if (response instanceof CacheFlushResponse.Success) {
      System.out.println("Cache 'test-cache' flushed");
    } else if (response instanceof CacheFlushResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to flush cache 'test-cache': " + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_Set(CacheClient cacheClient) {
    final SetResponse response = cacheClient.set("test-cache", "test-key", "test-value").join();
    if (response instanceof SetResponse.Success) {
      System.out.println("Key 'test-key' stored successfully");
    } else if (response instanceof SetResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to store key 'test-key' in cache 'test-cache': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_Get(CacheClient cacheClient) {
    final GetResponse response = cacheClient.get("test-cache", "test-key").join();
    if (response instanceof GetResponse.Hit hit) {
      System.out.println("Retrieved value for key 'test-key': " + hit.valueString());
    } else if (response instanceof GetResponse.Miss) {
      System.out.println("Key 'test-key' was not found in cache 'test-cache'");
    } else if (response instanceof GetResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to get key 'test-key' from cache 'test-cache': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_Delete(CacheClient cacheClient) {
    final DeleteResponse response = cacheClient.delete("test-cache", "test-key").join();
    if (response instanceof DeleteResponse.Success) {
      System.out.println("Key 'test-key' deleted successfully");
    } else if (response instanceof DeleteResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to delete key 'test-key' from cache 'test-cache': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_Increment(CacheClient cacheClient) {
    cacheClient.set("test-cache", "test-key", "10").join();
    final IncrementResponse response = cacheClient.increment("test-cache", "test-key", 1).join();
    if (response instanceof IncrementResponse.Success success) {
      System.out.println(
          "Key 'test-key' incremented successfully. New value in key test-key: "
              + success.valueNumber());
    } else if (response instanceof IncrementResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to increment the value of key 'test-key' from cache 'test-cache': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_SetIfNotExists(CacheClient cacheClient) {
    final SetIfNotExistsResponse response =
        cacheClient.setIfNotExists("test-cache", "test-key", "test-field").join();
    if (response instanceof SetIfNotExistsResponse.Stored) {
      System.out.println("Field 'test-field' set in key 'test-key'");
    } else if (response instanceof SetIfNotExistsResponse.NotStored) {
      System.out.println(
          "Key 'test-key' already exists in cache 'test-cache', so we did not overwrite it");
    } else if (response instanceof SetIfNotExistsResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to call setIfNotExists for the key 'test-key' in cache 'test-cache': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void main(String[] args) {
    example_API_CredentialProviderFromEnvVar();
    example_API_CredentialProviderFromString();
    example_API_ConfigurationLaptop();
    example_API_ConfigurationInRegionLatest();
    example_API_ConfigurationLowLatency();

    example_API_InstantiateCacheClient();
    try (final CacheClient cacheClient =
        CacheClient.builder(
                CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
                Configurations.Laptop.v1(),
                Duration.ofSeconds(60))
            .build()) {

      try {
        example_API_ErrorHandlingHitMiss(cacheClient);
      } catch (Exception e) {
        System.out.println("Hit/Miss error handling succeeded");
        System.out.println(e.getMessage());
      }
      try {
        example_API_ErrorHandlingSuccess(cacheClient);
      } catch (Exception e) {
        System.out.println("Success error handling succeeded");
        System.out.println(e.getMessage());
      }

      example_API_CreateCache(cacheClient);
      example_API_DeleteCache(cacheClient);
      example_API_CreateCache(cacheClient);
      example_API_ListCaches(cacheClient);
      example_API_FlushCache(cacheClient);

      example_API_Set(cacheClient);
      example_API_Get(cacheClient);
      example_API_Delete(cacheClient);
      example_API_Increment(cacheClient);
      example_API_SetIfNotExists(cacheClient);
    }
  }
}
