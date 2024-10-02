package momento.sdk;

import java.time.Duration;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.control.CacheCreateResponse;

public class Program {
  public static void main(String[] args) {
    CredentialProvider credentialProvider =
        CredentialProvider.withMomentoLocal(System.getenv("MOMENTO_API_KEY"));
    CacheClient cacheClient =
        new CacheClientBuilder(
                credentialProvider, Configurations.Laptop.latest(), Duration.ofMinutes(1))
            .build();

    CacheCreateResponse createResponse = cacheClient.createCache("cache").join();
    if (createResponse instanceof CacheCreateResponse.Success) {
      System.out.println("Cache created successfully");
    } else {
      System.out.println("Failed to create cache" + createResponse.toString());
      CacheCreateResponse.Error error = (CacheCreateResponse.Error) createResponse;
      System.out.println("Failed to create cache: " + error.getErrorCode());
    }
  }
}
