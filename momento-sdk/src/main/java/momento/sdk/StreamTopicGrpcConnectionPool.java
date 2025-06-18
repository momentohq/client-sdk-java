package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;
import java.util.concurrent.atomic.AtomicInteger;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.internal.GrpcChannelOptions;

interface StreamTopicGrpcConnectionPool {
  StreamStubWithCount getNextStreamStub();

  void close();
}

// Helper class for bookkeeping the number of active concurrent subscriptions.
final class StreamStubWithCount {
  private final PubsubGrpc.PubsubStub stub;
  private final AtomicInteger count = new AtomicInteger(0);

  StreamStubWithCount(PubsubGrpc.PubsubStub stub) {
    this.stub = stub;
  }

  PubsubGrpc.PubsubStub getStub() {
    return stub;
  }

  int getCount() {
    return count.get();
  }

  int incrementCount() {
    return count.incrementAndGet();
  }

  int decrementCount() {
    return count.decrementAndGet();
  }

  void acquireStubOrThrow() throws ClientSdkException {
    if (count.incrementAndGet() <= GrpcChannelOptions.NUM_CONCURRENT_STREAMS_PER_GRPC_CHANNEL) {
      return;
    } else {
      count.decrementAndGet();
      throw new ClientSdkException(
          MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED,
          "Maximum number of active subscriptions reached");
    }
  }
}
