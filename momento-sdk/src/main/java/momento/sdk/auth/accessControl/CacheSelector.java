package momento.sdk.auth.accessControl;

public abstract class CacheSelector {

    public static class SelectAllCaches extends CacheSelector {
    }

    public static final SelectAllCaches AllCaches = new SelectAllCaches();

    public static class SelectByCacheName extends CacheSelector {
        public final String CacheName;

        public SelectByCacheName(String cacheName) {
            CacheName = cacheName;
        }
    }

    public static SelectByCacheName ByName(String cacheName) {
        return new SelectByCacheName(cacheName);
    }
}
