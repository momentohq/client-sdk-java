package momento.sdk;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.GetBatchResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.SetBatchResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;

public class Program {
  public static void main(String[] args) {
    String cacheName = "test-cache";
    CredentialProvider credentialProvider =
        CredentialProvider.withMomentoLocal(System.getenv("MOMENTO_API_KEY"));

    Configuration configuration = Configurations.Laptop.latest();
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


    // Set requests
//    for (int i = 0; i < 10; i++) {
//      SetResponse setResponse =
//          cacheClient
//              .set(
//                  cacheName,
//                  "key" + i,
//                    "val" + i,
//                  Duration.ofMinutes(1))
//              .join();
//      if (setResponse instanceof SetResponse.Success) {
//        System.out.println("Set successful");
//      } else {
//        System.out.println("Failed to set batch" + setResponse.toString());
//        SetResponse.Error error = (SetResponse.Error) setResponse;
//        System.out.println("Failed to set: " + error.getErrorCode());
//      }
//    }
//
//    // Get requests
//    for (int i = 0; i < 10; i++) {
//      final GetResponse getResponse =
//          cacheClient.get(cacheName, "key" + i).join();
//      if (getResponse instanceof GetResponse.Hit) {
//        System.out.println("Get successful");
//        String value =
//            ((GetResponse.Hit) getResponse).valueString();
//        System.out.println("Value: " + value);
//      } else if (getResponse instanceof GetResponse.Miss) {
//        System.out.println("Get successful");
//        System.out.println("Value: null");
//      } else {
//        System.out.println("Failed to get" + getResponse.toString());
//        GetResponse.Error error = (GetResponse.Error) getResponse;
//        System.out.println("Failed to get: " + error.getErrorCode());
//      }
//    }

    final Map<String, String> items = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      items.put("key" + i, "val" + i);
    }
    final SetBatchResponse setBatchResponse =
        cacheClient.setBatch(cacheName, items, Duration.ofMinutes(1)).join();
    if (setBatchResponse instanceof SetBatchResponse.Success) {
      System.out.println("Set batch successful");
    } else {
      System.out.println("Failed to set batch" + setBatchResponse.toString());
      SetBatchResponse.Error error = (SetBatchResponse.Error) setBatchResponse;
      System.out.println("Failed to set batch: " + error.getErrorCode());
    }

    final GetBatchResponse getBatchResponse =
        cacheClient.getBatch(cacheName, items.keySet()).join();
    if (getBatchResponse instanceof GetBatchResponse.Success) {
      System.out.println("Get batch successful");
      Map<String, String> values =
          ((GetBatchResponse.Success) getBatchResponse).valueMapStringString();
      for (Map.Entry<String, String> entry : values.entrySet()) {
        System.out.println("Key: " + entry.getKey() + " Value: " + entry.getValue());
      }
    } else {
      System.out.println("Failed to get batch" + getBatchResponse.toString());
      GetBatchResponse.Error error = (GetBatchResponse.Error) getBatchResponse;
      System.out.println("Failed to get batch: " + error.getErrorCode());
    }
  }
}
