package momento.sdk;

import momento.sdk.responses.topic.TopicMessage;

public interface ISubscribeCallOptions {
  void onItem(TopicMessage message);

  void onCompleted();

  void onError(Throwable t);
}
