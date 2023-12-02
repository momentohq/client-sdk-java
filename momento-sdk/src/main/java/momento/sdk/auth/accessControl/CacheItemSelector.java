package momento.sdk.auth.accessControl;

public abstract class CacheItemSelector {

    public static class SelectAllCacheItems extends CacheItemSelector {
    }

    public static final SelectAllCacheItems AllCacheItems = new SelectAllCacheItems();

    public static class SelectByKey extends CacheItemSelector {
        public final String CacheKey;

        public SelectByKey(String cacheKey) {
            CacheKey = cacheKey;
        }
    }

    public static SelectByKey ByKey(String cacheKey) {
        return new SelectByKey(cacheKey);
    }

    public static class SelectByKeyPrefix extends CacheItemSelector {
        public final String CacheKeyPrefix;

        public SelectByKeyPrefix(String cacheKeyPrefix) {
            CacheKeyPrefix = cacheKeyPrefix;
        }
    }

    public static SelectByKeyPrefix ByKeyPrefix(String cacheKeyPrefix) {
        return new SelectByKeyPrefix(cacheKeyPrefix);
    }
}
