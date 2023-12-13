package momento.sdk;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import javax.annotation.Nullable;

public abstract class CancelableClientCallStreamObserver<TResp>
    extends ClientCallStreamObserver<TResp> implements ClientResponseObserver<Object, TResp> {

  private ClientCallStreamObserver requestStream;

  @Override
  public void cancel(@Nullable String message, @Nullable Throwable cause) {
    if (requestStream != null) {
      requestStream.cancel(message, cause);
    }
  }

  @Override
  public void beforeStart(ClientCallStreamObserver requestStream) {
    this.requestStream = requestStream;
  }
}
