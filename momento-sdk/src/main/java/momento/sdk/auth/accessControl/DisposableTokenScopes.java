package momento.sdk.auth.accessControl;

import java.util.Collections;
import java.util.List;

public class DisposableTokenScopes {
  private final List<DisposableTokenPermission> permissions;

  public DisposableTokenScopes(List<DisposableTokenPermission> permissions) {
    this.permissions = permissions;
  }

  public static DisposableTokenScope cacheReadWrite(String cacheName) {
    return cacheReadWrite(CacheSelector.ByName(cacheName));
  }

  public static DisposableTokenScope cacheReadWrite(CacheSelector cacheSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.CacheItemPermission(
                CacheRole.ReadWrite, cacheSelector, CacheItemSelector.AllCacheItems)));
  }

  public static DisposableTokenScope cacheReadOnly(String cacheName) {
    return cacheReadOnly(CacheSelector.ByName(cacheName));
  }

  public static DisposableTokenScope cacheReadOnly(CacheSelector cacheSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.CacheItemPermission(
                CacheRole.ReadOnly, cacheSelector, CacheItemSelector.AllCacheItems)));
  }

  public static DisposableTokenScope cacheWriteOnly(String cacheName) {
    return cacheWriteOnly(CacheSelector.ByName(cacheName));
  }

  public static DisposableTokenScope cacheWriteOnly(CacheSelector cacheSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.CacheItemPermission(
                CacheRole.WriteOnly, cacheSelector, CacheItemSelector.AllCacheItems)));
  }

  public static DisposableTokenScope cacheKeyReadWrite(String cacheName, String cacheKey) {
    return cacheKeyReadWrite(CacheSelector.ByName(cacheName), CacheItemSelector.ByKey(cacheKey));
  }

  public static DisposableTokenScope cacheKeyReadWrite(
      CacheSelector cacheSelector, String cacheKey) {
    return cacheKeyReadWrite(cacheSelector, CacheItemSelector.ByKey(cacheKey));
  }

  private static DisposableTokenScope cacheKeyReadWrite(
      CacheSelector cacheSelector, CacheItemSelector cacheItemSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.CacheItemPermission(
                CacheRole.ReadWrite, cacheSelector, cacheItemSelector)));
  }

  public static DisposableTokenScope cacheKeyReadOnly(String cacheName, String cacheKey) {
    return cacheKeyReadOnly(CacheSelector.ByName(cacheName), CacheItemSelector.ByKey(cacheKey));
  }

  public static DisposableTokenScope cacheKeyReadOnly(
      CacheSelector cacheSelector, String cacheKey) {
    return cacheKeyReadOnly(cacheSelector, CacheItemSelector.ByKey(cacheKey));
  }

  private static DisposableTokenScope cacheKeyReadOnly(
      CacheSelector cacheSelector, CacheItemSelector cacheItemSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.CacheItemPermission(
                CacheRole.ReadOnly, cacheSelector, cacheItemSelector)));
  }

  public static DisposableTokenScope cacheKeyWriteOnly(String cacheName, String cacheKey) {
    return cacheKeyWriteOnly(CacheSelector.ByName(cacheName), CacheItemSelector.ByKey(cacheKey));
  }

  public static DisposableTokenScope cacheKeyWriteOnly(
      CacheSelector cacheSelector, String cacheKey) {
    return cacheKeyWriteOnly(cacheSelector, CacheItemSelector.ByKey(cacheKey));
  }

  private static DisposableTokenScope cacheKeyWriteOnly(
      CacheSelector cacheSelector, CacheItemSelector cacheItemSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.CacheItemPermission(
                CacheRole.WriteOnly, cacheSelector, cacheItemSelector)));
  }

  public static DisposableTokenScope cacheKeyPrefixReadWrite(
      String cacheName, String cacheKeyPrefix) {
    return cacheKeyPrefixReadWrite(
        CacheSelector.ByName(cacheName), CacheItemSelector.ByKeyPrefix(cacheKeyPrefix));
  }

  public static DisposableTokenScope cacheKeyPrefixReadWrite(
      CacheSelector cacheSelector, String cacheKeyPrefix) {
    return cacheKeyPrefixReadWrite(cacheSelector, CacheItemSelector.ByKeyPrefix(cacheKeyPrefix));
  }

  private static DisposableTokenScope cacheKeyPrefixReadWrite(
      CacheSelector cacheSelector, CacheItemSelector cacheItemSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.CacheItemPermission(
                CacheRole.ReadWrite, cacheSelector, cacheItemSelector)));
  }

  public static DisposableTokenScope cacheKeyPrefixReadOnly(
      String cacheName, String cacheKeyPrefix) {
    return cacheKeyPrefixReadOnly(
        CacheSelector.ByName(cacheName), CacheItemSelector.ByKeyPrefix(cacheKeyPrefix));
  }

  public static DisposableTokenScope cacheKeyPrefixReadOnly(
      CacheSelector cacheSelector, String cacheKeyPrefix) {
    return cacheKeyPrefixReadOnly(cacheSelector, CacheItemSelector.ByKeyPrefix(cacheKeyPrefix));
  }

  private static DisposableTokenScope cacheKeyPrefixReadOnly(
      CacheSelector cacheSelector, CacheItemSelector cacheItemSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.CacheItemPermission(
                CacheRole.ReadOnly, cacheSelector, cacheItemSelector)));
  }

  public static DisposableTokenScope cacheKeyPrefixWriteOnly(
      String cacheName, String cacheKeyPrefix) {
    return cacheKeyPrefixWriteOnly(
        CacheSelector.ByName(cacheName), CacheItemSelector.ByKeyPrefix(cacheKeyPrefix));
  }

  public static DisposableTokenScope cacheKeyPrefixWriteOnly(
      CacheSelector cacheSelector, String cacheKeyPrefix) {
    return cacheKeyPrefixWriteOnly(cacheSelector, CacheItemSelector.ByKeyPrefix(cacheKeyPrefix));
  }

  private static DisposableTokenScope cacheKeyPrefixWriteOnly(
      CacheSelector cacheSelector, CacheItemSelector cacheItemSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.CacheItemPermission(
                CacheRole.WriteOnly, cacheSelector, cacheItemSelector)));
  }

  public static DisposableTokenScope topicPublishSubscribe(String cacheName, String topicName) {
    return topicPublishSubscribe(CacheSelector.ByName(cacheName), TopicSelector.ByName(topicName));
  }

  public static DisposableTokenScope topicPublishSubscribe(
      CacheSelector cacheSelector, String topicName) {
    return topicPublishSubscribe(cacheSelector, TopicSelector.ByName(topicName));
  }

  public static DisposableTokenScope topicPublishSubscribe(
      String cacheName, TopicSelector topicSelector) {
    return topicPublishSubscribe(CacheSelector.ByName(cacheName), topicSelector);
  }

  public static DisposableTokenScope topicPublishSubscribe(
      CacheSelector cacheSelector, TopicSelector topicSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.TopicPermission(
                TopicRole.PublishSubscribe, cacheSelector, topicSelector)));
  }

  public static DisposableTokenScope topicSubscribeOnly(String cacheName, String topicName) {
    return topicSubscribeOnly(CacheSelector.ByName(cacheName), TopicSelector.ByName(topicName));
  }

  public static DisposableTokenScope topicSubscribeOnly(
      CacheSelector cacheSelector, String topicName) {
    return topicSubscribeOnly(cacheSelector, TopicSelector.ByName(topicName));
  }

  public static DisposableTokenScope topicSubscribeOnly(
      String cacheName, TopicSelector topicSelector) {
    return topicSubscribeOnly(CacheSelector.ByName(cacheName), topicSelector);
  }

  public static DisposableTokenScope topicSubscribeOnly(
      CacheSelector cacheSelector, TopicSelector topicSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.TopicPermission(
                TopicRole.SubscribeOnly, cacheSelector, topicSelector)));
  }

  public static DisposableTokenScope topicPublishOnly(String cacheName, String topicName) {
    return topicPublishOnly(CacheSelector.ByName(cacheName), TopicSelector.ByName(topicName));
  }

  public static DisposableTokenScope topicPublishOnly(
      CacheSelector cacheSelector, String topicName) {
    return topicPublishOnly(cacheSelector, TopicSelector.ByName(topicName));
  }

  public static DisposableTokenScope topicPublishOnly(
      String cacheName, TopicSelector topicSelector) {
    return topicPublishOnly(CacheSelector.ByName(cacheName), topicSelector);
  }

  public static DisposableTokenScope topicPublishOnly(
      CacheSelector cacheSelector, TopicSelector topicSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.TopicPermission(
                TopicRole.PublishOnly, cacheSelector, topicSelector)));
  }

  public static DisposableTokenScope topicNamePrefixPublishSubscribe(
      String cacheName, String topicNamePrefix) {
    return topicNamePrefixPublishSubscribe(
        CacheSelector.ByName(cacheName), TopicSelector.ByTopicNamePrefix(topicNamePrefix));
  }

  public static DisposableTokenScope topicNamePrefixPublishSubscribe(
      CacheSelector cacheSelector, String topicNamePrefix) {
    return topicNamePrefixPublishSubscribe(
        cacheSelector, TopicSelector.ByTopicNamePrefix(topicNamePrefix));
  }

  public static DisposableTokenScope topicNamePrefixPublishSubscribe(
      String cacheName, TopicSelector topicSelector) {
    return topicNamePrefixPublishSubscribe(CacheSelector.ByName(cacheName), topicSelector);
  }

  public static DisposableTokenScope topicNamePrefixPublishSubscribe(
      CacheSelector cacheSelector, TopicSelector topicSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.TopicPermission(
                TopicRole.PublishSubscribe, cacheSelector, topicSelector)));
  }

  public static DisposableTokenScope topicNamePrefixSubscribeOnly(
      String cacheName, String topicNamePrefix) {
    return topicNamePrefixSubscribeOnly(
        CacheSelector.ByName(cacheName), TopicSelector.ByTopicNamePrefix(topicNamePrefix));
  }

  public static DisposableTokenScope topicNamePrefixSubscribeOnly(
      CacheSelector cacheSelector, String topicNamePrefix) {
    return topicNamePrefixSubscribeOnly(
        cacheSelector, TopicSelector.ByTopicNamePrefix(topicNamePrefix));
  }

  public static DisposableTokenScope topicNamePrefixSubscribeOnly(
      String cacheName, TopicSelector topicSelector) {
    return topicNamePrefixSubscribeOnly(CacheSelector.ByName(cacheName), topicSelector);
  }

  public static DisposableTokenScope topicNamePrefixSubscribeOnly(
      CacheSelector cacheSelector, TopicSelector topicSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.TopicPermission(
                TopicRole.SubscribeOnly, cacheSelector, topicSelector)));
  }

  public static DisposableTokenScope topicNamePrefixPublishOnly(
      String cacheName, String topicNamePrefix) {
    return topicNamePrefixPublishOnly(
        CacheSelector.ByName(cacheName), TopicSelector.ByTopicNamePrefix(topicNamePrefix));
  }

  public static DisposableTokenScope topicNamePrefixPublishOnly(
      CacheSelector cacheSelector, String topicNamePrefix) {
    return topicNamePrefixPublishOnly(
        cacheSelector, TopicSelector.ByTopicNamePrefix(topicNamePrefix));
  }

  public static DisposableTokenScope topicNamePrefixPublishOnly(
      String cacheName, TopicSelector topicSelector) {
    return topicNamePrefixPublishOnly(CacheSelector.ByName(cacheName), topicSelector);
  }

  public static DisposableTokenScope topicNamePrefixPublishOnly(
      CacheSelector cacheSelector, TopicSelector topicSelector) {
    return new DisposableTokenScope(
        Collections.singletonList(
            new DisposableToken.TopicPermission(
                TopicRole.PublishOnly, cacheSelector, topicSelector)));
  }
}
