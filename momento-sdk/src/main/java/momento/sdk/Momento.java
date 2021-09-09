package momento.sdk;

// TODO: Add Implementation and make non-abstract
public abstract class Momento {

    public static Momento init(String authToken, Regions region) {
        throw new UnsupportedOperationException();
    }

    public static Momento init(String authToken, String endpoint) {
        throw new UnsupportedOperationException();
    }

    public abstract void createCache(String cacheName);

    public abstract Cache getCache(String cacheName);

}
