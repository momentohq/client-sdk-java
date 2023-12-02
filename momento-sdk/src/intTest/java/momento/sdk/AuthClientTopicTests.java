package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import momento.sdk.auth.accessControl.CacheSelector;
import momento.sdk.auth.accessControl.DisposableTokenScopes;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.auth.accessControl.TopicSelector;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.responses.GenerateDisposableTokenResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AuthClientTopicTests extends BaseTestClass {
  private final TokenClient tokenClient = new TokenClient(credentialProvider);
  private AuthClient authClient;
  private String cacheName;
  private String topicName = "topic";

  @BeforeEach
  void setup() {
    authClient = new AuthClient(tokenClient);
    cacheName = System.getenv("TEST_CACHE_NAME");
  }

  @Test
  void generateDisposableTopicAuthTokenHappyPath() {
    try {
      // Topic Publish-Subscribe
      GenerateDisposableTokenResponse response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe(cacheName, topicName),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe(
                      CacheSelector.ByName(cacheName), topicName),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe(
                      cacheName, TopicSelector.ByName(topicName)),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe(
                      CacheSelector.ByName(cacheName), TopicSelector.ByName(topicName)),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Topic Subscribe-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicSubscribeOnly(cacheName, topicName),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicSubscribeOnly(
                      CacheSelector.ByName(cacheName), topicName),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicSubscribeOnly(
                      cacheName, TopicSelector.ByName(topicName)),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicSubscribeOnly(
                      CacheSelector.ByName(cacheName), TopicSelector.ByName(topicName)),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Topic Publish-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishOnly(cacheName, topicName),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishOnly(
                      CacheSelector.ByName(cacheName), topicName),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishOnly(
                      cacheName, TopicSelector.ByName(topicName)),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishOnly(
                      CacheSelector.ByName(cacheName), TopicSelector.ByName(topicName)),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Topic Prefix Publish-Subscribe
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixPublishSubscribe(cacheName, "topic-"),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixPublishSubscribe(
                      CacheSelector.ByName(cacheName), "topic-"),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixPublishSubscribe(
                      cacheName, TopicSelector.ByTopicNamePrefix("topic-")),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixPublishSubscribe(
                      CacheSelector.ByName(cacheName), TopicSelector.ByTopicNamePrefix("topic-")),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Topic Prefix Subscribe-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixSubscribeOnly(cacheName, "topic-"),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixSubscribeOnly(
                      CacheSelector.ByName(cacheName), "topic-"),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixSubscribeOnly(
                      cacheName, TopicSelector.ByTopicNamePrefix("topic-")),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixSubscribeOnly(
                      CacheSelector.ByName(cacheName), TopicSelector.ByTopicNamePrefix("topic-")),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      // Topic Prefix Publish-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixPublishOnly(cacheName, "topic-"),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixPublishOnly(
                      CacheSelector.ByName(cacheName), "topic-"),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixPublishOnly(
                      cacheName, TopicSelector.ByTopicNamePrefix("topic-")),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Success,
          "Unexpected response: " + response);

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicNamePrefixPublishOnly(
                      CacheSelector.ByName(cacheName), TopicSelector.ByTopicNamePrefix("topic-")),
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
  void generateDisposableTopicAuthTokenErrorsOnNull() {
    try {
      // Topic Publish-Only
      GenerateDisposableTokenResponse response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishOnly((String) null, topicName),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishOnly(cacheName, (String) null),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      // Topic Subscribe-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicSubscribeOnly((String) null, topicName),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicSubscribeOnly(cacheName, (String) null),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      // Topic Publish-Subscribe
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe((String) null, topicName),
                  ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe(cacheName, (String) null),
                  ExpiresIn.minutes(10))
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
  void generateDisposableTopicAuthTokenErrorsOnEmpty() {
    try {
      // Topic Publish-Only
      GenerateDisposableTokenResponse response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishOnly("", topicName), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishOnly(cacheName, ""), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      // Topic Subscribe-Only
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicSubscribeOnly("", topicName), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicSubscribeOnly(cacheName, ""), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      // Topic Publish-Subscribe
      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe("", topicName), ExpiresIn.minutes(10))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe(cacheName, ""), ExpiresIn.minutes(10))
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
  void generateDisposableTopicAuthTokenErrorsOnBadExpiry() {
    try {
      GenerateDisposableTokenResponse response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe(cacheName, topicName),
                  ExpiresIn.minutes(0))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe(cacheName, topicName),
                  ExpiresIn.minutes(-50))
              .join();
      assertTrue(
          response instanceof GenerateDisposableTokenResponse.Error,
          "Unexpected response: " + response);
      assertEquals(
          MomentoErrorCode.INVALID_ARGUMENT_ERROR,
          ((GenerateDisposableTokenResponse.Error) response).getErrorCode());

      response =
          authClient
              .generateDisposableTokenAsync(
                  DisposableTokenScopes.topicPublishSubscribe(cacheName, topicName),
                  ExpiresIn.minutes(365))
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
}
