package momento.sdk.messages;

import com.google.protobuf.ByteString;
import grpc.cache_client.ECacheResult;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class CacheGetResponse extends BaseResponse {
  private final ByteString body;
  private final ECacheResult result;

  public CacheGetResponse(ECacheResult result, ByteString body) {
    this.body = body;
    this.result = result;
  }

  public MomentoCacheResult result() {
    return this.resultMapper(this.result);
  }

  public Optional<byte[]> byteArray() {
    if (result != ECacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.toByteArray());
  }

  public Optional<ByteBuffer> byteBuffer() {
    if (result != ECacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.asReadOnlyByteBuffer());
  }

  /**
   * Converts the value read from cache to a UTF-8 String
   *
   * @return
   */
  public Optional<String> string() {
    return string(StandardCharsets.UTF_8);
  }

  public Optional<String> string(Charset charset) {
    if (result != ECacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.toString(charset));
  }

  public Optional<InputStream> inputStream() {
    if (result != ECacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.newInput());
  }
}
