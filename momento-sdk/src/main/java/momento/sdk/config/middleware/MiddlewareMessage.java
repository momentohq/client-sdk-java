package momento.sdk.config.middleware;

import com.google.protobuf.Message;

public class MiddlewareMessage {
  private final Message grpcMessage;

  public MiddlewareMessage(Message message) {
    this.grpcMessage = message;
  }

  public int getMessageLength() {
    return grpcMessage != null ? grpcMessage.toByteArray().length : 0;
  }

  public String getConstructorName() {
    return grpcMessage.getClass().getSimpleName();
  }

  public Message getMessage() {
    return grpcMessage;
  }
}
