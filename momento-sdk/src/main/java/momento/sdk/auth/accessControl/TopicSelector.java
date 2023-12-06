package momento.sdk.auth.accessControl;

public abstract class TopicSelector {

  public static class SelectAllTopics extends TopicSelector {}

  public static final SelectAllTopics AllTopics = new SelectAllTopics();

  public static class SelectByTopicName extends TopicSelector {
    public final String TopicName;

    public SelectByTopicName(String topicName) {
      TopicName = topicName;
    }
  }

  public static SelectByTopicName ByName(String topicName) {
    return new SelectByTopicName(topicName);
  }

  public static class SelectByTopicNamePrefix extends TopicSelector {
    public final String TopicNamePrefix;

    public SelectByTopicNamePrefix(String topicNamePrefix) {
      TopicNamePrefix = topicNamePrefix;
    }
  }

  public static SelectByTopicNamePrefix ByTopicNamePrefix(String topicNamePrefix) {
    return new SelectByTopicNamePrefix(topicNamePrefix);
  }
}
