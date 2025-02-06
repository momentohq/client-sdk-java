package momento.sdk;

import java.time.Duration;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.MomentoLocalProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.Configurations;
import momento.sdk.config.middleware.Middleware;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;

public class Program {
  public static void main(String[] args) {
    String cacheName = "test-cache-java";
    CredentialProvider credentialProvider = new MomentoLocalProvider();

    Middleware customMiddleware = new MyCustomMiddleware();
    Configuration configuration = Configurations.Laptop.latest().withMiddleware(customMiddleware);

    CacheClient cacheClient =
        new CacheClientBuilder(credentialProvider, configuration, Duration.ofMinutes(1)).build();

    CacheCreateResponse createResponse = cacheClient.createCache(cacheName).join();
    if (createResponse instanceof CacheCreateResponse.Success) {
      System.out.println("Cache created successfully");
    } else {
      System.out.println("Failed to create cache" + createResponse.toString());
      CacheCreateResponse.Error error = (CacheCreateResponse.Error) createResponse;
      System.out.println("Failed to create cache: " + error.getErrorCode());
    }

    SetResponse setResponse = cacheClient.set(cacheName, "key", "value").join();
    if (setResponse instanceof SetResponse.Success) {
      System.out.println("Set key successfully");
    } else {
      System.out.println("Failed to set key" + setResponse.toString());
      SetResponse.Error error = (SetResponse.Error) setResponse;
      System.out.println("Failed to set key: " + error.getErrorCode());
      cacheClient.close();
    }

    GetResponse getResponse = cacheClient.get(cacheName, "key").join();
    if (getResponse instanceof GetResponse.Hit) {
      GetResponse.Hit hit = (GetResponse.Hit) getResponse;
      System.out.println("Got value: " + hit.value());
    } else if (getResponse instanceof GetResponse.Miss) {
      System.out.println("Key not found");
    } else {
      System.out.println("Failed to get key" + getResponse.toString());
      GetResponse.Error error = (GetResponse.Error) getResponse;
      System.out.println("Failed to get key: " + error.getErrorCode());
      cacheClient.close();
    }

    cacheClient.close();
  }
}
