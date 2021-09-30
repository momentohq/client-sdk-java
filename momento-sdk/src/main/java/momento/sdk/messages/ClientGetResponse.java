package momento.sdk.messages;

import com.google.protobuf.ByteString;
import grpc.cache_client.ECacheResult;
import java.nio.ByteBuffer;

public final class ClientGetResponse extends BaseResponse {
  private final ByteString body;
  private final ECacheResult result;

  public ClientGetResponse(ECacheResult result, ByteString body) {
    this.body = body;
    this.result = result;
  }

  public MomentoCacheResult getResult() {
    return this.resultMapper(this.result);
  }

  public byte[] asByteArray() {
    return body.toByteArray();
  }

  public ByteBuffer asByteBuffer() {
    return body.asReadOnlyByteBuffer();
  }

  public String asStringUtf8() {
    return body.toStringUtf8();
  }
}
