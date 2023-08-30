package momento.client.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheListResponse;

class SimpleRepro {

  public static void main(String... args) {
    final CredentialProvider credentialProvider =
        new EnvVarCredentialProvider("MOMENTO_AUTH_TOKEN");

    List<CacheClient> clients = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      clients.add(
          new CacheClient(
              credentialProvider, Configurations.Laptop.latest(), Duration.ofSeconds(60)));
      System.out.println("Created client " + i);
    }

    for (int i = 0; i < 1000; i++) {
      final CacheClient client = clients.get(i % 2);
      final CacheCreateResponse createResponse =
              client.createCache(String.valueOf(i)).join();
      System.out.println(createResponse);
      final CacheListResponse listResponse = client.listCaches().join();
      if (createResponse instanceof CacheCreateResponse.Success) {
        System.out.println(client.deleteCache(String.valueOf(i)).join());
      }
    }
  }
}
