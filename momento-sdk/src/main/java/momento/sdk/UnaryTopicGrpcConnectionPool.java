package momento.sdk;

import grpc.cache_client.pubsub.PubsubGrpc;

interface UnaryTopicGrpcConnectionPool {
  PubsubGrpc.PubsubStub getNextUnaryStub();

  void close();
}
