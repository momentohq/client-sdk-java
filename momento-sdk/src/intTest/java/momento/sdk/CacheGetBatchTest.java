package momento.sdk;

import java.time.Duration;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import org.junit.jupiter.api.Test;

final class CacheGetBatchTest {
  @Test
  public void getBatchSetBatchHappyPath() {
    CredentialProvider credentialProvider =
        CredentialProvider.withMomentoLocal(System.getenv("MOMENTO_API_KEY"));
    CacheClient cacheClient =
        new CacheClientBuilder(
                credentialProvider, Configurations.Laptop.latest(), Duration.ofMinutes(1))
            .build();

    CacheCreateResponse resp = cacheClient.createCache("cache").join();
    System.out.println(resp);
    //    SetResponse setCacheResponse = cacheClient.set("cache", "key1", "val1",
    // Duration.ofMinutes(1)).join();
    //    System.out.println(setCacheResponse);

    //    final Map<String, String> items = new HashMap<>();
    //    items.put("key1", "val1");
    //    items.put("key2", "val2");
    //    items.put("key3", "val3");
    //    final SetBatchResponse setBatchResponse =
    //        cacheClient.setBatch("cache", items, Duration.ofMinutes(1)).join();
    //    assertThat(setBatchResponse).isInstanceOf(SetBatchResponse.Success.class);
    //    for (SetResponse setResponse :
    //        ((SetBatchResponse.Success) setBatchResponse).results().values()) {
    //      assertThat(setResponse).isInstanceOf(SetResponse.Success.class);
    //    }
    //
    //    final GetBatchResponse getBatchResponse = cacheClient.getBatch("cache",
    // items.keySet()).join();
    //
    //    assertThat(getBatchResponse).isInstanceOf(GetBatchResponse.Success.class);
    //    assertThat(((GetBatchResponse.Success) getBatchResponse).valueMapStringString())
    //        .containsExactlyEntriesOf(items);
  }
}
