package momento.client.example.doc_examples;

import java.time.Duration;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.GetResponse;

public class ReadmeExample {
  public static void main(String[] args) {
    try (final CacheClient cacheClient =
        CacheClient.create(
            CredentialProvider.fromEnvVar("MOMENTO_AUTH_TOKEN"),
            Configurations.Laptop.v1(),
            Duration.ofSeconds(60))) {
      final String cacheName = "cache";

      cacheClient.createCache(cacheName).join();

      cacheClient.set(cacheName, "foo", "bar").join();

      final GetResponse response = cacheClient.get(cacheName, "foo").join();
      if (response instanceof GetResponse.Hit hit) {
        System.out.println("Got value: " + hit.valueString());
      }
    }
  }
}
