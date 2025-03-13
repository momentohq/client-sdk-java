package momento.sdk.retry;

import java.util.HashMap;
import java.util.Map;

public enum MomentoRpcMethod {
  GET("_GetRequest"),
  SET("_SetRequest"),
  DELETE("_DeleteRequest"),
  INCREMENT("_IncrementRequest"),
  SET_IF("_SetIfRequest"),
  SET_IF_NOT_EXISTS("_SetIfNotExistsRequest"),
  GET_BATCH("_GetBatchRequest"),
  SET_BATCH("_SetBatchRequest"),
  KEYS_EXIST("_KeysExistRequest"),
  UPDATE_TTL("_UpdateTtlRequest"),
  ITEM_GET_TTL("_ItemGetTtlRequest"),
  ITEM_GET_TYPE("_ItemGetTypeRequest"),
  DICTIONARY_GET("_DictionaryGetRequest"),
  DICTIONARY_FETCH("_DictionaryFetchRequest"),
  DICTIONARY_SET("_DictionarySetRequest"),
  DICTIONARY_INCREMENT("_DictionaryIncrementRequest"),
  DICTIONARY_DELETE("_DictionaryDeleteRequest"),
  DICTIONARY_LENGTH("_DictionaryLengthRequest"),
  SET_FETCH("_SetFetchRequest"),
  SET_SAMPLE("_SetSampleRequest"),
  SET_UNION("_SetUnionRequest"),
  SET_DIFFERENCE("_SetDifferenceRequest"),
  SET_CONTAINS("_SetContainsRequest"),
  SET_LENGTH("_SetLengthRequest"),
  SET_POP("_SetPopRequest"),
  LIST_PUSH_FRONT("_ListPushFrontRequest"),
  LIST_PUSH_BACK("_ListPushBackRequest"),
  LIST_POP_FRONT("_ListPopFrontRequest"),
  LIST_POP_BACK("_ListPopBackRequest"),
  LIST_ERASE("_ListEraseRequest"),
  LIST_REMOVE("_ListRemoveRequest"),
  LIST_FETCH("_ListFetchRequest"),
  LIST_LENGTH("_ListLengthRequest"),
  LIST_CONCATENATE_FRONT("_ListConcatenateFrontRequest"),
  LIST_CONCATENATE_BACK("_ListConcatenateBackRequest"),
  LIST_RETAIN("_ListRetainRequest"),
  SORTED_SET_PUT("_SortedSetPutRequest"),
  SORTED_SET_FETCH("_SortedSetFetchRequest"),
  SORTED_SET_GET_SCORE("_SortedSetGetScoreRequest"),
  SORTED_SET_REMOVE("_SortedSetRemoveRequest"),
  SORTED_SET_INCREMENT("_SortedSetIncrementRequest"),
  SORTED_SET_GET_RANK("_SortedSetGetRankRequest"),
  SORTED_SET_LENGTH("_SortedSetLengthRequest"),
  SORTED_SET_LENGTH_BY_SCORE("_SortedSetLengthByScoreRequest"),
  TOPIC_PUBLISH("_PublishRequest"),
  TOPIC_SUBSCRIBE("_SubscriptionRequest");

  private final String requestName;

  private static final Map<String, MomentoRpcMethod> lookup = new HashMap<>();

  static {
    for (MomentoRpcMethod method : MomentoRpcMethod.values()) {
      lookup.put(method.getRequestName(), method);
    }
  }

  /**
   * Constructor for MomentoRpcMethod.
   *
   * @param requestName - The request name.
   */
  MomentoRpcMethod(String requestName) {
    this.requestName = requestName;
  }

  /**
   * Returns the request name.
   *
   * @return The request name.
   */
  public String getRequestName() {
    return requestName;
  }

  /**
   * Returns the MomentoRpcMethod from the request name.
   *
   * @param requestName - The request name.
   * @return The MomentoRpcMethod.
   */
  public static MomentoRpcMethod fromString(String requestName) {
    return lookup.getOrDefault(requestName, null);
  }
}
