package momento.sdk.responses.storage.control;

import java.util.Arrays;
import java.util.List;
import momento.sdk.exceptions.SdkException;

/** Response for a list stores operation */
public interface ListStoresResponse {

  /** A successful list stores operation. */
  class Success implements ListStoresResponse {
    private final List<StoreInfo> stores;

    public Success(List<StoreInfo> stores) {
      this.stores = stores;
    }

    // TODO delete stub constructor
    public Success() {
      this.stores = Arrays.asList(new StoreInfo("myStore"), new StoreInfo("testStore"));
    }

    public List<StoreInfo> getStores() {
      return stores;
    }
  }

  /**
   * A failed list stores operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements ListStoresResponse {

    /**
     * Constructs a list store error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
