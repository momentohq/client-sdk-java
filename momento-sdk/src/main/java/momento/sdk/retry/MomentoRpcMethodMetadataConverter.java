package momento.sdk.retry;

import java.util.HashMap;
import java.util.Map;

public class MomentoRpcMethodMetadataConverter {
  private static final Map<MomentoRpcMethod, String> rpcMethodToMetadataMap = new HashMap<>();

  static {
    rpcMethodToMetadataMap.put(MomentoRpcMethod.GET, "get");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET, "set");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.DELETE, "delete");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.INCREMENT, "increment");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_IF, "set-if");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_IF_NOT_EXISTS, "set-if");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.GET_BATCH, "get-batch");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_BATCH, "set-batch");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.KEYS_EXIST, "keys-exist");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.UPDATE_TTL, "update-ttl");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.ITEM_GET_TTL, "item-get-ttl");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.ITEM_GET_TYPE, "item-get-type");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.DICTIONARY_SET, "dictionary-set");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.DICTIONARY_GET, "dictionary-get");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.DICTIONARY_FETCH, "dictionary-fetch");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.DICTIONARY_INCREMENT, "dictionary-increment");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.DICTIONARY_DELETE, "dictionary-delete");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.DICTIONARY_LENGTH, "dictionary-length");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_FETCH, "set-fetch");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_SAMPLE, "set-sample");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_UNION, "set-union");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_DIFFERENCE, "set-difference");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_CONTAINS, "set-contains");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_LENGTH, "set-length");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SET_POP, "set-pop");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_PUSH_FRONT, "list-push-front");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_PUSH_BACK, "list-push-back");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_POP_FRONT, "list-pop-front");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_POP_BACK, "list-pop-back");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_ERASE, "list-remove"); // Alias for list-remove
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_REMOVE, "list-remove");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_FETCH, "list-fetch");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_LENGTH, "list-length");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_CONCATENATE_FRONT, "list-concatenate-front");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_CONCATENATE_BACK, "list-concatenate-back");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.LIST_RETAIN, "list-retain");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SORTED_SET_PUT, "sorted-set-put");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SORTED_SET_FETCH, "sorted-set-fetch");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SORTED_SET_GET_SCORE, "sorted-set-get-score");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SORTED_SET_REMOVE, "sorted-set-remove");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SORTED_SET_INCREMENT, "sorted-set-increment");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SORTED_SET_GET_RANK, "sorted-set-get-rank");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.SORTED_SET_LENGTH, "sorted-set-length");
    rpcMethodToMetadataMap.put(
        MomentoRpcMethod.SORTED_SET_LENGTH_BY_SCORE, "sorted-set-length-by-score");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.TOPIC_PUBLISH, "topic-publish");
    rpcMethodToMetadataMap.put(MomentoRpcMethod.TOPIC_SUBSCRIBE, "topic-subscribe");
  }

  public static String convert(MomentoRpcMethod rpcMethod) {
    if (!rpcMethodToMetadataMap.containsKey(rpcMethod)) {
      throw new IllegalArgumentException("Unsupported MomentoRpcMethod: " + rpcMethod);
    }
    return rpcMethodToMetadataMap.get(rpcMethod);
  }
}
