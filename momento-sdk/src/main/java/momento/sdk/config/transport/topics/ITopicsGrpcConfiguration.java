package momento.sdk.config.transport.topics;

import momento.sdk.config.transport.IGrpcConfiguration;

public interface ITopicsGrpcConfiguration extends IGrpcConfiguration {

  /**
   * The number of stream gRPC channels to keep open at any given time.
   *
   * @return the number of stream gRPC channels.
   */
  int getNumStreamGrpcChannels();

  /**
   * The number of unary gRPC channels to keep open at any given time.
   *
   * @return the number of unary gRPC channels.
   */
  int getNumUnaryGrpcChannels();
}
