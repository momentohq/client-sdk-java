package momento.sdk.responses.storage.control;

import java.util.Arrays;
import java.util.List;
import momento.sdk.exceptions.SdkException;

/** Response for a list persistent stores operation */
public interface ListPersistentStoresResponse {

  /** A successful list persistent stores operation. */
  class Success implements ListPersistentStoresResponse {
    private final List<StoreInfo> stores;

    public Success() {
      this.stores = Arrays.asList(new StoreInfo("myStore"), new StoreInfo("testStore"));
    }

    public List<StoreInfo> getStores() {
      return stores;
    }
  }

  /**
   * A failed list persistent stores operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements ListPersistentStoresResponse {

    /**
     * Constructs a persistent store delete error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
