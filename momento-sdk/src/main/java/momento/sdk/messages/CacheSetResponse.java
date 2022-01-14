package momento.sdk.messages;

import com.google.protobuf.ByteString;
import grpc.cache_client.ECacheResult;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/** Result of the set operation on Cache. */
public final class CacheSetResponse extends BaseResponse {
  private final ECacheResult result;
  private final ByteString value;

  public CacheSetResponse(ECacheResult result, ByteString value) {
    this.result = result;
    this.value = value;
  }

  /**
   * Result of the Set Operation on Cache.
   *
   * @return result of the set operation. {@link MomentoCacheResult#Ok} is the only valid status.
   *     All other results should be considered an error.
   */
  public MomentoCacheResult result() {
    return this.resultMapper(this.result);
  }

  /**
   * Value set in the cache as a byte array.
   *
   * @return Value set for the given key.
   */
  public Optional<byte[]> byteArray() {
    return Optional.ofNullable(value.toByteArray());
  }

  /**
   * Value set in the cache as a {@link ByteBuffer}.
   *
   * @return Value set for the given key.
   */
  public Optional<ByteBuffer> byteBuffer() {
    return Optional.ofNullable(value.asReadOnlyByteBuffer());
  }

  /**
   * Value set in the cache as a UTF-8 {@link String}
   *
   * @return Value set for the given key.
   */
  public Optional<String> string() {
    return string(StandardCharsets.UTF_8);
  }

  /**
   * Value set in the cache as {@link String}.
   *
   * @param charset to express the bytes as String.
   */
  public Optional<String> string(Charset charset) {
    return Optional.ofNullable(value.toString(charset));
  }

  /**
   * Value as an {@link InputStream}
   *
   * @return Value stored for the given key.
   */
  public Optional<InputStream> inputStream() {
    return Optional.ofNullable(value.newInput());
  }
}
