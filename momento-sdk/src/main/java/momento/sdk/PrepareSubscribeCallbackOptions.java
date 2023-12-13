package momento.sdk;

import momento.sdk.responses.topic.TopicSubscribeResponse;

/** Encapsulates parameters for the subscribe callback prepare methods. */
public interface PrepareSubscribeCallbackOptions {
  /** The promise resolve function. */
  void resolve(TopicSubscribeResponse.Subscription value);

  /** The promise reject function. */
  void reject(TopicSubscribeResponse.Error error);

  /**
   * Whether the stream was restarted due to an error. If so, we skip the end stream handler logic
   * as the error handler will have restarted the stream.
   */
  boolean isRestartedDueToError();

  /** If the first message is an error, we return an error immediately and do not subscribe. */
  boolean isFirstMessage();
}
