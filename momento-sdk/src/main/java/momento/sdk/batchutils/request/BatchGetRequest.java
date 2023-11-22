package momento.sdk.batchutils.request;

import java.util.Collection;

public class BatchGetRequest<T> {
  private final Collection<T> keys;

  public BatchGetRequest(Collection<T> keys) {
    this.keys = keys;
  }

  public Collection<T> getKeys() {
    return keys;
  }

  // String Key Wrapper
  public static class StringKeyBatchGetRequest extends BatchGetRequest<String> {
    public StringKeyBatchGetRequest(Collection<String> keys) {
      super(keys);
    }
  }

  // Byte Array Key Wrapper
  public static class ByteArrayKeyBatchGetRequest extends BatchGetRequest<byte[]> {
    public ByteArrayKeyBatchGetRequest(Collection<byte[]> keys) {
      super(keys);
    }
  }
}
