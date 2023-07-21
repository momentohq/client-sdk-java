package momento.sdk.retry;

import com.google.common.base.Preconditions;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import javax.annotation.Nullable;

/**
 * A custom implementation of {@link ClientCall} that handles retrying unary (single request, single
 * response) operations. This class is used by the RetryClientInterceptor to manage retry logic for
 * failed gRPC calls.
 *
 * <p>The {@code RetryingUnaryClientCall} wraps an original {@link ClientCall} and intercepts the
 * methods related to starting, sending messages, and handling the response. If the original call
 * encounters an error, the interceptor schedules a retry attempt based on the configured retry
 * strategy and eligibility rules.
 *
 * <p>Each instance of {@code RetryingUnaryClientCall} maintains its own state, including the
 * request message, response listener, headers, and other properties specific to the call. When a
 * retry is needed, a new instance of this class is created with the original request details, and
 * the retry attempt is initiated.
 *
 * <p>Note that this implementation assumes that the gRPC call is unary, meaning the client sends
 * one message and receives one response. For streaming calls or other call types, a different
 * approach or implementation would be required.
 */
public class RetryingUnaryClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

  private ClientCall<ReqT, RespT> delegate;
  private Listener<RespT> responseListener;
  private Metadata headers;
  private ReqT message;
  private int numMessages;
  private boolean compressionEnabled;

  /**
   * Constructs a new instance of {@code RetryingUnaryClientCall} with the provided delegate.
   *
   * @param delegate The original {@link ClientCall} to be wrapped and managed by this retrying
   *     call.
   */
  public RetryingUnaryClientCall(final ClientCall<ReqT, RespT> delegate) {
    this.delegate = delegate;
  }

  @Override
  public void start(Listener<RespT> responseListener, Metadata headers) {
    Preconditions.checkNotNull(responseListener, "responseListener cannot be null");
    Preconditions.checkNotNull(headers, "Headers cannot be null");
    this.responseListener = responseListener;
    this.headers = headers;
    this.delegate.start(responseListener, headers);
  }

  @Override
  public void request(int numMessages) {
    this.numMessages = numMessages;
    this.delegate.request(numMessages);
  }

  @Override
  public void cancel(@Nullable String message, @Nullable Throwable cause) {
    this.delegate.cancel(message, cause);
  }

  @Override
  public void halfClose() {
    this.delegate.halfClose();
  }

  @Override
  public void setMessageCompression(boolean enabled) {
    this.compressionEnabled = enabled;
    this.delegate.setMessageCompression(enabled);
  }

  @Override
  public void sendMessage(ReqT message) {
    Preconditions.checkState(this.message == null, "Expecting only one message to be sent");
    this.message = message;
    this.delegate.sendMessage(message);
  }

  @Override
  public boolean isReady() {
    return delegate.isReady();
  }

  /**
   * This method is called by the RetryClientInterceptor's listener if there was an error in the
   * original request. It is responsible for scheduling a retry attempt with the same request
   * details as the original call.
   *
   * @param delegate The new {@link ClientCall} instance to be used for the retry attempt.
   */
  public void retry(ClientCall<ReqT, RespT> delegate) {
    this.delegate = delegate;
    try {
      this.delegate.start(responseListener, headers);
      this.delegate.setMessageCompression(compressionEnabled);
      this.delegate.request(numMessages);
      this.delegate.sendMessage(message);
      this.delegate.halfClose();
    } catch (Throwable t) {
      // we try to cancel the request on any errors
      this.delegate.cancel(t.getMessage(), t);
    }
  }
}
