package momento.client.example;

import java.time.Duration;
import momento.sdk.CacheClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;

public abstract class AbstractExample {

  protected static final String AUTH_TOKEN_ENV_VAR = "MOMENTO_AUTH_TOKEN";
  protected static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  protected static CacheClient buildCacheClient() {
    final CredentialProvider credentialProvider = new EnvVarCredentialProvider(AUTH_TOKEN_ENV_VAR);

    return CacheClient.builder(credentialProvider, Configurations.Laptop.Latest(), DEFAULT_ITEM_TTL)
        .build();
  }

  protected static void printStartBanner(String exampleType) {
    System.out.println("******************************************************************");
    System.out.println(exampleType + " Example Start");
    System.out.println("******************************************************************");
  }

  protected static void printEndBanner(String exampleType) {
    System.out.println("******************************************************************");
    System.out.println(exampleType + " Example End");
    System.out.println("******************************************************************");
  }
}
