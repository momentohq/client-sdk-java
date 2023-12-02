package momento.sdk.auth.accessControl;

public abstract class DisposableToken {
    public static class CachePermission extends DisposableTokenPermission {
        private final CacheRole role;
        private final CacheSelector cacheSelector;

        public CachePermission(CacheRole role, CacheSelector cacheSelector) {
            this.role = role;
            this.cacheSelector = cacheSelector;
        }

        public CacheRole getRole() {
            return role;
        }

        public CacheSelector getCacheSelector() {
            return cacheSelector;
        }
    }

    public static class CacheItemPermission extends CachePermission {
        private final CacheItemSelector cacheItemSelector;

        public CacheItemPermission(CacheRole role, CacheSelector cacheSelector, CacheItemSelector cacheItemSelector) {
            super(role, cacheSelector);
            this.cacheItemSelector = cacheItemSelector;
        }

        public CacheItemSelector getCacheItemSelector() {
            return cacheItemSelector;
        }
    }

    public static class TopicPermission extends DisposableTokenPermission {
        private final TopicRole role;
        private final CacheSelector cacheSelector;
        private final TopicSelector topicSelector;

        public TopicPermission(TopicRole role, CacheSelector cacheSelector, TopicSelector topicSelector) {
            this.role = role;
            this.cacheSelector = cacheSelector;
            this.topicSelector = topicSelector;
        }

        public TopicRole getRole() {
            return role;
        }

        public CacheSelector getCacheSelector() {
            return cacheSelector;
        }

        public TopicSelector getTopicSelector() {
            return topicSelector;
        }
    }
}
