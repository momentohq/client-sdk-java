package momento.client.example.doc_examples;

import java.time.Duration;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;

public class CheatSheet {
  public static void main(String[] args) {
    try (final CacheClient cacheClient =
        CacheClient.create(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            Configurations.Laptop.v1(),
            Duration.ofSeconds(60) /* defaultTTL for your cache items*/,
            Duration.ofSeconds(10) /* eagerConnectionTimeout, default is 30 seconds */)) {
      // ...
    }
  }
}
