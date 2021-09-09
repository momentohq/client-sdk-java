package momento.sdk;

// TODO: Add Implementation and make non-abstract
public abstract class Momento {

    public static Momento init(String authToken) {
        throw new UnsupportedOperationException();
    }

    public abstract void createCache(String cacheName);

    public abstract Cache getCache(String cacheName);

}
