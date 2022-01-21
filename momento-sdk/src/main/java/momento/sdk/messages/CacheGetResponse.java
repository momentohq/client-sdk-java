package momento.sdk.messages;

import com.google.protobuf.ByteString;
import grpc.cache_client.ECacheResult;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import momento.sdk.exceptions.InternalServerException;

/** Response for a cache get operation */
public final class CacheGetResponse {
  private final ByteString body;
  private final MomentoCacheResult result;

  public CacheGetResponse(ECacheResult result, ByteString body) {
    this.body = body;
    this.result = convert(result);
  }

  /**
   * Determine the result of the Get operation.
   *
   * <p>Valid values are {@link MomentoCacheResult#Hit} and {@link MomentoCacheResult#Miss}.
   *
   * @return The result of Cache Get Operation
   */
  public MomentoCacheResult result() {
    return result;
  }

  /**
   * Value stored in the cache as a byte array.
   *
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<byte[]> byteArray() {
    if (result != MomentoCacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.toByteArray());
  }

  /**
   * Value stored in the cache as a {@link ByteBuffer}.
   *
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<ByteBuffer> byteBuffer() {
    if (result != MomentoCacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.asReadOnlyByteBuffer());
  }

  /**
   * Value stored in the cache as a UTF-8 {@link String}
   *
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<String> string() {
    return string(StandardCharsets.UTF_8);
  }

  /**
   * Value stored in the cache as {@link String}.
   *
   * @param charset to express the bytes as String.
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<String> string(Charset charset) {
    if (result != MomentoCacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.toString(charset));
  }

  /**
   * Value as an {@link InputStream}
   *
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<InputStream> inputStream() {
    if (result != MomentoCacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.newInput());
  }

  private static MomentoCacheResult convert(ECacheResult result) {
    switch (result) {
      case Hit:
        return MomentoCacheResult.Hit;
      case Miss:
        return MomentoCacheResult.Miss;
      default:
        throw new InternalServerException(
            String.format(
                "Unexpected exception occurred while trying to fulfill the request. Found unsupported Cache result: %s",
                result));
    }
  }
}
