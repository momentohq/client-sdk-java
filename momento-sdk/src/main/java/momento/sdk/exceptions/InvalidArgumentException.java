package momento.sdk.exceptions;

public class InvalidArgumentException extends CacheServiceException {
    public InvalidArgumentException(String message) {
        super(message);
    }
}
