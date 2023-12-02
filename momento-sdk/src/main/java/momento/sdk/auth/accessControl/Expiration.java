package momento.sdk.auth.accessControl;

public abstract class Expiration {
    private final boolean doesExpire;

    protected Expiration(boolean doesExpire) {
        this.doesExpire = doesExpire;
    }

    public boolean doesExpire() {
        return doesExpire;
    }
}

