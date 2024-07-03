package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import momento.sdk.auth.StringCredentialProvider;
import momento.sdk.auth.accessControl.CacheItemSelector;
import momento.sdk.auth.accessControl.CacheRole;
import momento.sdk.auth.accessControl.CacheSelector;
import momento.sdk.auth.accessControl.DisposableToken;
import momento.sdk.auth.accessControl.DisposableTokenPermission;
import momento.sdk.auth.accessControl.DisposableTokenScope;
import momento.sdk.auth.accessControl.DisposableTokenScopes;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.responses.auth.GenerateDisposableTokenResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthClientCacheTests extends BaseTestClass {
  private static AuthClient authClient;
  private static CacheClient cacheClient;

  private String cacheName;

  String key = "test-key";
  String value = "test-value";
  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);

  @BeforeAll
  static void setup() {
    authClient = AuthClient.builder(credentialProvider).build();
    cacheClient =
        CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
            .build();
    cacheName = testCacheName();
  }

  private CompletableFuture<CacheClient> getClientForTokenScope(DisposableTokenScope scope) {
    return authClient
        .generateDisposableTokenAsync(scope, ExpiresIn.minutes(2))
        .thenCompose(
            response -> {
              if (response instanceof GenerateDisposableTokenResponse.Success) {
                GenerateDisposableTokenResponse.Success token =
                    (GenerateDisposableTokenResponse.Success) response;
                assert !token.authToken().isEmpty();
                String authToken = token.authToken();
                StringCredentialProvider authProvider = new StringCredentialProvider(authToken);
                return CompletableFuture.completedFuture(
                    new CacheClient(
                        authProvider, Configurations.Laptop.latest(), Duration.ofSeconds(10)));
              } else {
                fail("Unexpected response: " + response);
                return CompletableFuture.completedFuture(null);
              }
            });
  }

  @Test
  void generateDisposableCacheAuthTokenHappyPath() {
    try {
      // Cache Read-Write
      GenerateDisposableTokenResponse response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheReadWrite(cacheName), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheReadWrite(CacheSelector.ByName(cacheName)),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Cache Read-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheReadOnly("cache"), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheReadOnly(CacheSelector.ByName("cache")),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Cache Write-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheWriteOnly("cache"), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheWriteOnly(CacheSelector.ByName("cache")),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getMessage());
    }
  }

  @Test
  void generateDisposableCacheAuthTokenErrorsOnNull() {
    try {
      // Cache Read-Write
      GenerateDisposableTokenResponse response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheReadWrite((String) null), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      // Cache Read-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheReadOnly((String) null), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      // Cache Write-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheWriteOnly((String) null), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getMessage());
    }
  }

  @Test
  void generateDisposableCacheAuthTokenErrorsOnEmpty() {
    try {
      // Cache Read-Write
      GenerateDisposableTokenResponse response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheReadWrite(""), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      // Cache Read-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheReadOnly(""), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      // Cache Write-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheWriteOnly(""), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getMessage());
    }
  }

  @Test
  void generateDisposableCacheAuthTokenReadWriteHappyPath()
      throws ExecutionException, InterruptedException {
    CacheClient readwriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheReadWrite(cacheName)).get();

    SetResponse setResponse = readwriteCacheClient.set(cacheName, key, value).get();
    assertTrue(setResponse instanceof SetResponse.Success, "Unexpected response: " + setResponse);

    GetResponse getResponse = readwriteCacheClient.get(cacheName, key).get();
    assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
    GetResponse.Hit hit = (GetResponse.Hit) getResponse;
    assertEquals(value, hit.valueString());

    readwriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheReadWrite("someothercache")).get();
    setResponse = readwriteCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());

    getResponse = readwriteCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());
  }

  @Test
  void generateDisposableCacheAuthTokenReadOnlyHappyPath()
      throws ExecutionException, InterruptedException {
    CacheClient readWriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheReadWrite(cacheName)).get();

    CacheClient readOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheReadOnly(cacheName)).get();

    SetResponse setResponse = readOnlyCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());

    SetResponse setResponseForVerifyingGetResponse =
        readWriteCacheClient.set(cacheName, key, value).get();
    assertTrue(
        setResponseForVerifyingGetResponse instanceof SetResponse.Success,
        "Unexpected response: " + setResponseForVerifyingGetResponse);

    GetResponse getResponse = readOnlyCacheClient.get(cacheName, key).get();
    assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
    GetResponse.Hit hit = (GetResponse.Hit) getResponse;
    assertEquals(value, hit.valueString());
  }

  @Test
  void generateDisposableCacheAuthTokenWriteOnlyHappyPath()
      throws ExecutionException, InterruptedException {
    CacheClient readWriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheReadWrite(cacheName)).get();

    CacheClient writeOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheWriteOnly(cacheName)).get();

    SetResponse setResponse = writeOnlyCacheClient.set(cacheName, key, value).get();
    assertTrue(setResponse instanceof SetResponse.Success, "Unexpected response: " + setResponse);

    GetResponse getResponse = writeOnlyCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

    getResponse = readWriteCacheClient.get(cacheName, key).get();
    assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
    GetResponse.Hit hit = (GetResponse.Hit) getResponse;
    assertEquals(value, hit.valueString());

    writeOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheWriteOnly("someothercache")).get();

    setResponse = writeOnlyCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyAuthTokenHappyPath() {
    try {
      // Cache Read-Write
      GenerateDisposableTokenResponse response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyReadWrite(cacheName, key), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyReadWrite(CacheSelector.ByName(cacheName), key),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Cache Read-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyReadOnly(cacheName, key), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyReadOnly(CacheSelector.ByName(cacheName), key),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Cache Write-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyWriteOnly(cacheName, key), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyWriteOnly(CacheSelector.ByName(cacheName), key),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getMessage());
    }
  }

  @Test
  void generateDisposableCacheKeyAuthTokenErrorsOnNull()
      throws ExecutionException, InterruptedException {
    GenerateDisposableTokenResponse responseReadWrite =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadWrite(cacheName, null), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadWrite instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadWrite);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadWrite).getErrorCode());

    responseReadWrite =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadWrite((String) null, key), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadWrite instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadWrite);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadWrite).getErrorCode());

    GenerateDisposableTokenResponse responseReadOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadOnly(cacheName, null), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadOnly).getErrorCode());

    responseReadOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadOnly((String) null, key), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadOnly).getErrorCode());

    GenerateDisposableTokenResponse responseWriteOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyWriteOnly(cacheName, null), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseWriteOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseWriteOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseWriteOnly).getErrorCode());

    responseWriteOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyWriteOnly((String) null, key), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseWriteOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseWriteOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseWriteOnly).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyAuthTokenErrorsOnEmpty()
      throws ExecutionException, InterruptedException {
    GenerateDisposableTokenResponse responseReadWrite =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadWrite(cacheName, ""), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadWrite instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadWrite);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadWrite).getErrorCode());

    responseReadWrite =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadWrite("", key), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadWrite instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadWrite);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadWrite).getErrorCode());

    GenerateDisposableTokenResponse responseReadOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadOnly(cacheName, ""), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadOnly).getErrorCode());

    responseReadOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyReadOnly("", key), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadOnly).getErrorCode());

    GenerateDisposableTokenResponse responseWriteOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyWriteOnly(cacheName, ""), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseWriteOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseWriteOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseWriteOnly).getErrorCode());

    responseWriteOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyWriteOnly("", key), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseWriteOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseWriteOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseWriteOnly).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyAuthTokenReadWriteHappyPath()
      throws ExecutionException, InterruptedException {
    CacheClient readwriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyReadWrite(cacheName, key)).get();

    SetResponse setResponse = readwriteCacheClient.set(cacheName, key, value).get();
    assertTrue(setResponse instanceof SetResponse.Success, "Unexpected response: " + setResponse);

    GetResponse getResponse = readwriteCacheClient.get(cacheName, key).get();
    assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
    GetResponse.Hit hit = (GetResponse.Hit) getResponse;
    assertEquals(value, hit.valueString());

    readwriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyReadWrite("someothercache", key))
            .get();
    setResponse = readwriteCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
    getResponse = readwriteCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

    readwriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyReadWrite(cacheName, "someotherkey"))
            .get();
    setResponse = readwriteCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
    getResponse = readwriteCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyAuthTokenReadOnlyHappyPath()
      throws ExecutionException, InterruptedException {
    CacheClient readOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyReadOnly(cacheName, key)).get();

    SetResponse setResponse = readOnlyCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());

    CacheClient readWriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyReadWrite(cacheName, key)).get();
    readWriteCacheClient.set(cacheName, key, value).get();

    GetResponse getResponse = readOnlyCacheClient.get(cacheName, key).get();
    assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
    GetResponse.Hit hit = (GetResponse.Hit) getResponse;
    assertEquals(value, hit.valueString());

    readOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyReadOnly("someothercache", key)).get();
    getResponse = readOnlyCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

    readOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyReadWrite(cacheName, "someotherkey"))
            .get();
    getResponse = readOnlyCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyAuthTokenWriteOnlyHappyPath()
      throws ExecutionException, InterruptedException {
    CacheClient writeOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyWriteOnly(cacheName, key)).get();

    SetResponse setResponse = writeOnlyCacheClient.set(cacheName, key, value).get();
    assertTrue(setResponse instanceof SetResponse.Success, "Unexpected response: " + setResponse);
    GetResponse getResponse = writeOnlyCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

    writeOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyWriteOnly("someothercache", key))
            .get();
    setResponse = writeOnlyCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());

    writeOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyWriteOnly(cacheName, "someotherkey"))
            .get();
    setResponse = writeOnlyCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyPrefixAuthTokenHappyPath() {
    try {
      // Cache Read-Write
      GenerateDisposableTokenResponse response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyPrefixReadWrite(cacheName, key),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyPrefixReadWrite(
                      CacheSelector.ByName(cacheName), key),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Cache Read-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyPrefixReadOnly(cacheName, key),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyPrefixReadOnly(
                      CacheSelector.ByName(cacheName), key),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Cache Write-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyPrefixWriteOnly(cacheName, key),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.cacheKeyPrefixWriteOnly(
                      CacheSelector.ByName(cacheName), key),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getMessage());
    }
  }

  @Test
  void generateDisposableCacheKeyPrefixAuthTokenErrorsOnNull()
      throws ExecutionException, InterruptedException {
    GenerateDisposableTokenResponse responseReadWrite =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixReadWrite(cacheName, null),
                ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadWrite instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadWrite);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadWrite).getErrorCode());

    responseReadWrite =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixReadWrite((String) null, key),
                ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadWrite instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadWrite);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadWrite).getErrorCode());

    GenerateDisposableTokenResponse responseReadOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixReadOnly(cacheName, null),
                ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadOnly).getErrorCode());

    responseReadOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixReadOnly((String) null, null),
                ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadOnly).getErrorCode());

    GenerateDisposableTokenResponse responseWriteOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixWriteOnly(cacheName, null),
                ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseWriteOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseWriteOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseWriteOnly).getErrorCode());

    responseWriteOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixWriteOnly((String) null, key),
                ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseWriteOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseWriteOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseWriteOnly).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyPrefixAuthTokenErrorsOnEmpty()
      throws ExecutionException, InterruptedException {
    GenerateDisposableTokenResponse responseReadWrite =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixReadWrite(cacheName, ""), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadWrite instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadWrite);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadWrite).getErrorCode());

    responseReadWrite =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixReadWrite("", key), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadWrite instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadWrite);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadWrite).getErrorCode());

    GenerateDisposableTokenResponse responseReadOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixReadOnly(cacheName, ""), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadOnly).getErrorCode());

    responseReadOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixReadOnly("", key), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseReadOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseReadOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseReadOnly).getErrorCode());

    GenerateDisposableTokenResponse responseWriteOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixWriteOnly(cacheName, ""), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseWriteOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseWriteOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseWriteOnly).getErrorCode());

    responseWriteOnly =
        authClient
            .generateDisposableTokenAsync(
                DisposableTokenScopes.cacheKeyPrefixWriteOnly("", key), ExpiresIn.minutes(10))
            .get();
    assertTrue(
        responseWriteOnly instanceof GenerateDisposableTokenResponse.Error,
        "Unexpected response: " + responseWriteOnly);
    assertEquals(
        MomentoErrorCode.INVALID_ARGUMENT_ERROR,
        ((GenerateDisposableTokenResponse.Error) responseWriteOnly).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyPrefixAuthTokenReadWriteHappyPath()
      throws ExecutionException, InterruptedException {
    CacheClient readwriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyPrefixReadWrite(cacheName, key)).get();

    SetResponse setResponse = readwriteCacheClient.set(cacheName, key, value).get();
    assertTrue(setResponse instanceof SetResponse.Success, "Unexpected response: " + setResponse);

    GetResponse getResponse = readwriteCacheClient.get(cacheName, key).get();
    assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
    GetResponse.Hit hit = (GetResponse.Hit) getResponse;
    assertEquals(value, hit.valueString());

    readwriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyPrefixReadWrite("someothercache", key))
            .get();
    setResponse = readwriteCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
    getResponse = readwriteCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

    readwriteCacheClient =
        getClientForTokenScope(
                DisposableTokenScopes.cacheKeyPrefixReadWrite(cacheName, "someotherkey"))
            .get();
    setResponse = readwriteCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
    getResponse = readwriteCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyPrefixAuthTokenReadOnlyHappyPath()
      throws ExecutionException, InterruptedException {
    CacheClient readOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyPrefixReadOnly(cacheName, key)).get();

    SetResponse setResponse = readOnlyCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());

    CacheClient readWriteCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyPrefixReadWrite(cacheName, key)).get();
    readWriteCacheClient.set(cacheName, key, value).get();

    GetResponse getResponse = readOnlyCacheClient.get(cacheName, key).get();
    assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
    GetResponse.Hit hit = (GetResponse.Hit) getResponse;
    assertEquals(value, hit.valueString());

    readOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyPrefixReadOnly("someothercache", key))
            .get();
    getResponse = readOnlyCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

    readOnlyCacheClient =
        getClientForTokenScope(
                DisposableTokenScopes.cacheKeyPrefixReadOnly(cacheName, "someotherkey"))
            .get();
    getResponse = readOnlyCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());
  }

  @Test
  void generateDisposableCacheKeyPrefixAuthTokenWriteOnlyHappyPath()
      throws ExecutionException, InterruptedException {
    CacheClient writeOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyPrefixWriteOnly(cacheName, key)).get();

    SetResponse setResponse = writeOnlyCacheClient.set(cacheName, key, value).get();
    assertTrue(setResponse instanceof SetResponse.Success, "Unexpected response: " + setResponse);
    GetResponse getResponse = writeOnlyCacheClient.get(cacheName, key).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

    writeOnlyCacheClient =
        getClientForTokenScope(DisposableTokenScopes.cacheKeyPrefixWriteOnly("someothercache", key))
            .get();
    setResponse = writeOnlyCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());

    writeOnlyCacheClient =
        getClientForTokenScope(
                DisposableTokenScopes.cacheKeyPrefixWriteOnly(cacheName, "someotherkey"))
            .get();
    setResponse = writeOnlyCacheClient.set(cacheName, key, value).get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
  }

  // Tests using DisposableTokenScopes composed of multiple permissions

  @Test
  void generateDisposableMultiPermissionScopeReadWriteWithSelectors()
      throws ExecutionException, InterruptedException {
    // Read/write permissions with selectors
    List<DisposableTokenPermission> permissions = new ArrayList<>();
    permissions.add(
        new DisposableToken.CacheItemPermission(
            CacheRole.ReadWrite, CacheSelector.ByName(cacheName), CacheItemSelector.ByKey("cow")));
    permissions.add(
        new DisposableToken.CacheItemPermission(
            CacheRole.ReadWrite,
            CacheSelector.ByName(cacheName),
            CacheItemSelector.ByKeyPrefix("pet")));
    DisposableTokenScope scope = new DisposableTokenScope(permissions);

    CacheClient client = getClientForTokenScope(scope).get();

    // Test read/write on specified key and key prefix
    SetResponse setResponse = client.set(cacheName, "cow", "moo").get();
    assertTrue(setResponse instanceof SetResponse.Success, "Unexpected response: " + setResponse);
    GetResponse getResponse = client.get(cacheName, "cow").get();
    assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
    GetResponse.Hit hit = (GetResponse.Hit) getResponse;
    assertEquals("moo", hit.valueString());

    setResponse = client.set(cacheName, "pet-cat", "meow").get();
    assertTrue(setResponse instanceof SetResponse.Success, "Unexpected response: " + setResponse);
    getResponse = client.get(cacheName, "pet-cat").get();
    assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
    GetResponse.Hit hit2 = (GetResponse.Hit) getResponse;
    assertEquals("meow", hit2.valueString());

    // Test read/write on a different cache or unspecified key/prefix
    setResponse = client.set(cacheName, "giraffe", "noidea").get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
    getResponse = client.get(cacheName, "giraffe").get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

    // Test read/write on specified key and key prefix to a different cache
    permissions = new ArrayList<>();
    permissions.add(
        new DisposableToken.CacheItemPermission(
            CacheRole.ReadWrite,
            CacheSelector.ByName("a-totally-different-cache"),
            CacheItemSelector.ByKey("cow")));
    permissions.add(
        new DisposableToken.CacheItemPermission(
            CacheRole.ReadWrite,
            CacheSelector.ByName("a-totally-different-cache"),
            CacheItemSelector.ByKeyPrefix("pet")));
    scope = new DisposableTokenScope(permissions);

    client = getClientForTokenScope(scope).get();

    setResponse = client.set(cacheName, "cow", "moo").get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
    getResponse = client.get(cacheName, "cow").get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

    setResponse = client.set(cacheName, "pet-cat", "meow").get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
    getResponse = client.get(cacheName, "pet-cat").get();
    assertEquals(
        MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());
  }

  @Test
  void generateDisposableMultiPermissionReadOnlyWithSelectorsAllCaches() {
    String cache2Name = cacheName + "-2";
    try {
      CacheCreateResponse createCacheResponse = cacheClient.createCache(cache2Name).get();
      assertTrue(
          createCacheResponse instanceof CacheCreateResponse.Success,
          "Unexpected response: " + createCacheResponse);

      List<DisposableTokenPermission> permissions = new ArrayList<>();
      permissions.add(
          new DisposableToken.CacheItemPermission(
              CacheRole.ReadOnly, CacheSelector.AllCaches, CacheItemSelector.ByKey("cow")));
      permissions.add(
          new DisposableToken.CacheItemPermission(
              CacheRole.ReadOnly, CacheSelector.AllCaches, CacheItemSelector.ByKeyPrefix("pet")));
      DisposableTokenScope scope = new DisposableTokenScope(permissions);
      CacheClient client = getClientForTokenScope(scope).get();

      // sets should fail for both caches
      SetResponse setResponse = client.set(cacheName, "cow", "moo").get();
      assertEquals(
          MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());
      setResponse = client.set(cache2Name, "pet-koala", "awwww").get();
      assertEquals(
          MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());

      // gets should succeed for specified key and key prefix but fail for other keys
      cacheClient.set(cacheName, "cow", "moo").get();
      cacheClient.set(cacheName, "pet-koala", "awww").get();
      cacheClient.set(cacheName, "dog", "woof").get();
      cacheClient.set(cache2Name, "cow", "moo").get();
      cacheClient.set(cache2Name, "pet-koala", "awww").get();
      cacheClient.set(cache2Name, "dog", "woof").get();

      GetResponse getResponse = client.get(cacheName, "cow").get();
      assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
      GetResponse.Hit hit = (GetResponse.Hit) getResponse;
      assertEquals("moo", hit.valueString());

      getResponse = client.get(cacheName, "pet-koala").get();
      assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
      hit = (GetResponse.Hit) getResponse;
      assertEquals("awww", hit.valueString());

      getResponse = client.get(cacheName, "dog").get();
      assertEquals(
          MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());

      getResponse = client.get(cache2Name, "cow").get();
      assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
      hit = (GetResponse.Hit) getResponse;
      assertEquals("moo", hit.valueString());

      getResponse = client.get(cache2Name, "pet-koala").get();
      assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
      hit = (GetResponse.Hit) getResponse;
      assertEquals("awww", hit.valueString());

      getResponse = client.get(cache2Name, "dog").get();
      assertEquals(
          MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getMessage());
    } finally {
      CacheDeleteResponse response = cacheClient.deleteCache(cache2Name).join();
      assertTrue(
          response instanceof CacheDeleteResponse.Success, "Unexpected response: " + response);
    }
  }

  @Test
  void generateDisposableMultiPermissionReadOnlyWriteOnly() {
    String cache2Name = cacheName + "-2";
    try {
      CacheCreateResponse createCacheResponse = cacheClient.createCache(cache2Name).get();
      assertTrue(
          createCacheResponse instanceof CacheCreateResponse.Success,
          "Unexpected response: " + createCacheResponse);

      List<DisposableTokenPermission> permissions = new ArrayList<>();
      permissions.add(
          new DisposableToken.CacheItemPermission(
              CacheRole.WriteOnly,
              CacheSelector.ByName(cacheName),
              CacheItemSelector.ByKey("cow")));
      permissions.add(
          new DisposableToken.CacheItemPermission(
              CacheRole.ReadOnly,
              CacheSelector.ByName(cache2Name),
              CacheItemSelector.ByKeyPrefix("pet")));
      DisposableTokenScope scope = new DisposableTokenScope(permissions);
      CacheClient client = getClientForTokenScope(scope).get();

      // we can write to only one key and not read in test cache
      SetResponse setResponse = client.set(cacheName, "cow", "moo").get();
      assertTrue(setResponse instanceof SetResponse.Success, "Unexpected response: " + setResponse);
      GetResponse getResponse = client.get(cacheName, "cow").get();
      assertEquals(
          MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());
      setResponse = client.set(cacheName, "parrot", "somethingaboutcrackers").get();
      assertEquals(
          MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());

      GetResponse verifyGetResponse = cacheClient.get(cacheName, "cow").get();
      assertTrue(
          verifyGetResponse instanceof GetResponse.Hit,
          "Unexpected response: " + verifyGetResponse);
      GetResponse.Hit hit = (GetResponse.Hit) verifyGetResponse;
      assertEquals("moo", hit.valueString());

      // we can read prefixed keys but no others and cannot write in the second cache
      cacheClient.set(cache2Name, "pet-armadillo", "thunk").get();
      cacheClient.set(cache2Name, "snake", "hiss").get();

      getResponse = client.get(cache2Name, "pet-armadillo").get();
      assertTrue(getResponse instanceof GetResponse.Hit, "Unexpected response: " + getResponse);
      hit = (GetResponse.Hit) getResponse;
      assertEquals("thunk", hit.valueString());

      setResponse = client.set(cache2Name, "pet-armadillo", "thunk").get();
      assertEquals(
          MomentoErrorCode.PERMISSION_ERROR, ((SetResponse.Error) setResponse).getErrorCode());

      getResponse = client.get(cache2Name, "snake").get();
      assertEquals(
          MomentoErrorCode.PERMISSION_ERROR, ((GetResponse.Error) getResponse).getErrorCode());
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getMessage());
    } finally {
      CacheDeleteResponse response = cacheClient.deleteCache(cache2Name).join();
      assertTrue(
          response instanceof CacheDeleteResponse.Success, "Unexpected response: " + response);
    }
  }
}
