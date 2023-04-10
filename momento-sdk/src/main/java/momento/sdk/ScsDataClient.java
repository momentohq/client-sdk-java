package momento.sdk;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;
import static momento.sdk.ValidationUtils.checkCacheNameValid;
import static momento.sdk.ValidationUtils.checkDictionaryNameValid;
import static momento.sdk.ValidationUtils.checkIndexRangeValid;
import static momento.sdk.ValidationUtils.checkListNameValid;
import static momento.sdk.ValidationUtils.checkScoreRangeValid;
import static momento.sdk.ValidationUtils.checkSetNameValid;
import static momento.sdk.ValidationUtils.checkSortedSetCountValid;
import static momento.sdk.ValidationUtils.checkSortedSetOffsetValid;
import static momento.sdk.ValidationUtils.ensureValidCacheSet;
import static momento.sdk.ValidationUtils.ensureValidKey;
import static momento.sdk.ValidationUtils.ensureValidValue;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import grpc.cache_client.ECacheResult;
import grpc.cache_client.ScsGrpc;
import grpc.cache_client._DeleteRequest;
import grpc.cache_client._DeleteResponse;
import grpc.cache_client._DictionaryDeleteRequest;
import grpc.cache_client._DictionaryDeleteResponse;
import grpc.cache_client._DictionaryFetchRequest;
import grpc.cache_client._DictionaryFetchResponse;
import grpc.cache_client._DictionaryFieldValuePair;
import grpc.cache_client._DictionaryGetRequest;
import grpc.cache_client._DictionaryGetResponse;
import grpc.cache_client._DictionaryIncrementRequest;
import grpc.cache_client._DictionaryIncrementResponse;
import grpc.cache_client._DictionarySetRequest;
import grpc.cache_client._DictionarySetResponse;
import grpc.cache_client._GetRequest;
import grpc.cache_client._GetResponse;
import grpc.cache_client._IncrementRequest;
import grpc.cache_client._IncrementResponse;
import grpc.cache_client._ListConcatenateBackRequest;
import grpc.cache_client._ListConcatenateBackResponse;
import grpc.cache_client._ListConcatenateFrontRequest;
import grpc.cache_client._ListConcatenateFrontResponse;
import grpc.cache_client._ListFetchRequest;
import grpc.cache_client._ListFetchResponse;
import grpc.cache_client._ListLengthRequest;
import grpc.cache_client._ListLengthResponse;
import grpc.cache_client._ListPopBackRequest;
import grpc.cache_client._ListPopBackResponse;
import grpc.cache_client._ListPopFrontRequest;
import grpc.cache_client._ListPopFrontResponse;
import grpc.cache_client._ListPushBackRequest;
import grpc.cache_client._ListPushBackResponse;
import grpc.cache_client._ListPushFrontRequest;
import grpc.cache_client._ListPushFrontResponse;
import grpc.cache_client._ListRemoveRequest;
import grpc.cache_client._ListRemoveResponse;
import grpc.cache_client._ListRetainRequest;
import grpc.cache_client._ListRetainResponse;
import grpc.cache_client._SetDifferenceRequest;
import grpc.cache_client._SetDifferenceResponse;
import grpc.cache_client._SetFetchRequest;
import grpc.cache_client._SetFetchResponse;
import grpc.cache_client._SetIfNotExistsRequest;
import grpc.cache_client._SetIfNotExistsResponse;
import grpc.cache_client._SetRequest;
import grpc.cache_client._SetResponse;
import grpc.cache_client._SetUnionRequest;
import grpc.cache_client._SetUnionResponse;
import grpc.cache_client._SortedSetElement;
import grpc.cache_client._SortedSetFetchRequest;
import grpc.cache_client._SortedSetFetchResponse;
import grpc.cache_client._SortedSetPutRequest;
import grpc.cache_client._SortedSetPutResponse;
import grpc.cache_client._Unbounded;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.messages.CacheDeleteResponse;
import momento.sdk.messages.CacheDictionaryFetchResponse;
import momento.sdk.messages.CacheDictionaryGetFieldResponse;
import momento.sdk.messages.CacheDictionaryGetFieldsResponse;
import momento.sdk.messages.CacheDictionaryIncrementResponse;
import momento.sdk.messages.CacheDictionaryRemoveFieldResponse;
import momento.sdk.messages.CacheDictionaryRemoveFieldsResponse;
import momento.sdk.messages.CacheDictionarySetFieldResponse;
import momento.sdk.messages.CacheDictionarySetFieldsResponse;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheIncrementResponse;
import momento.sdk.messages.CacheListConcatenateBackResponse;
import momento.sdk.messages.CacheListConcatenateFrontResponse;
import momento.sdk.messages.CacheListFetchResponse;
import momento.sdk.messages.CacheListLengthResponse;
import momento.sdk.messages.CacheListPopBackResponse;
import momento.sdk.messages.CacheListPopFrontResponse;
import momento.sdk.messages.CacheListPushBackResponse;
import momento.sdk.messages.CacheListPushFrontResponse;
import momento.sdk.messages.CacheListRemoveValueResponse;
import momento.sdk.messages.CacheListRetainResponse;
import momento.sdk.messages.CacheSetAddElementResponse;
import momento.sdk.messages.CacheSetAddElementsResponse;
import momento.sdk.messages.CacheSetFetchResponse;
import momento.sdk.messages.CacheSetIfNotExistsResponse;
import momento.sdk.messages.CacheSetRemoveElementResponse;
import momento.sdk.messages.CacheSetRemoveElementsResponse;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.CacheSortedSetFetchResponse;
import momento.sdk.messages.CacheSortedSetPutElementResponse;
import momento.sdk.messages.CacheSortedSetPutElementsResponse;
import momento.sdk.requests.CollectionTtl;

/** Client for interacting with Scs Data plane. */
final class ScsDataClient implements Closeable {

  private static final Metadata.Key<String> CACHE_NAME_KEY =
      Metadata.Key.of("cache", ASCII_STRING_MARSHALLER);

  private final Duration itemDefaultTtl;
  private final ScsDataGrpcStubsManager scsDataGrpcStubsManager;

  ScsDataClient(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull Configuration configuration,
      @Nonnull Duration defaultTtl) {
    this.itemDefaultTtl = defaultTtl;
    this.scsDataGrpcStubsManager = new ScsDataGrpcStubsManager(credentialProvider, configuration);
  }

  CompletableFuture<CacheGetResponse> get(String cacheName, byte[] key) {
    try {
      ensureValidKey(key);
      return sendGet(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheGetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheGetResponse> get(String cacheName, String key) {
    try {
      ensureValidKey(key);
      return sendGet(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheGetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDeleteResponse> delete(String cacheName, byte[] key) {
    try {
      ensureValidKey(key);
      return sendDelete(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDeleteResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDeleteResponse> delete(String cacheName, String key) {
    try {
      ensureValidKey(key);
      return sendDelete(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDeleteResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, ByteBuffer value, @Nullable Duration ttl) {
    try {
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      ensureValidCacheSet(key, value, ttl);
      return sendSet(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetResponse> set(
      String cacheName, byte[] key, byte[] value, @Nullable Duration ttl) {
    try {
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      ensureValidCacheSet(key, value, ttl);
      return sendSet(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetResponse> set(
      String cacheName, String key, String value, @Nullable Duration ttl) {
    try {
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      ensureValidCacheSet(key, value, ttl);
      return sendSet(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, String field, long amount, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendIncrement(cacheName, convert(field), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheIncrementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheIncrementResponse> increment(
      String cacheName, byte[] field, long amount, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendIncrement(cacheName, convert(field), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheIncrementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, String value, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, byte[] value, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, String value, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, byte[] value, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetAddElementResponse> setAddElement(
      String cacheName, String setName, String element, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElement(cacheName, convert(setName), convert(element), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetAddElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetAddElementResponse> setAddElement(
      String cacheName, String setName, byte[] element, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElement(cacheName, convert(setName), convert(element), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetAddElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetAddElementsResponse> setAddElements(
      String cacheName, String setName, Set<String> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(setName);
      ensureValidValue(elements);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElements(cacheName, convert(setName), convertStringSet(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetAddElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetAddElementsResponse> setAddElementsByteArray(
      String cacheName, String setName, Set<byte[]> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(setName);
      ensureValidValue(elements);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElements(cacheName, convert(setName), convertByteArraySet(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetAddElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetRemoveElementResponse> setRemoveElement(
      String cacheName, String setName, String element) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      return sendSetRemoveElement(cacheName, convert(setName), convert(element));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetRemoveElementResponse> setRemoveElement(
      String cacheName, String setName, byte[] element) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      return sendSetRemoveElement(cacheName, convert(setName), convert(element));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetRemoveElementsResponse> setRemoveElements(
      String cacheName, String setName, Set<String> elements) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(elements);
      return sendSetRemoveElements(cacheName, convert(setName), convertStringSet(elements));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetRemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetRemoveElementsResponse> setRemoveElementsByteArray(
      String cacheName, String setName, Set<byte[]> elements) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(elements);
      return sendSetRemoveElements(cacheName, convert(setName), convertByteArraySet(elements));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetRemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetFetchResponse> setFetch(String cacheName, String setName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(setName);
      return sendSetFetch(cacheName, convert(setName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      String cacheName,
      String sortedSetName,
      String element,
      double score,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(sortedSetName);
      ensureValidValue(element);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendSortedSetPutElement(
          cacheName, convert(sortedSetName), convert(element), score, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetPutElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetPutElementResponse> sortedSetPutElement(
      String cacheName,
      String sortedSetName,
      byte[] element,
      double score,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(sortedSetName);
      ensureValidValue(element);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendSortedSetPutElement(
          cacheName, convert(sortedSetName), convert(element), score, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetPutElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElements(
      String cacheName,
      String sortedSetName,
      Map<String, Double> elements,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(sortedSetName);
      ensureValidValue(elements);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendSortedSetPutElements(
          cacheName, convert(sortedSetName), convertStringScoreMap(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetPutElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElementsByteArray(
      String cacheName,
      String sortedSetName,
      Map<byte[], Double> elements,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(sortedSetName);
      ensureValidValue(elements);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendSortedSetPutElements(
          cacheName, convert(sortedSetName), convertBytesScoreMap(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetPutElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByRank(
      String cacheName,
      String sortedSetName,
      @Nullable Integer startRank,
      @Nullable Integer endRank) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(sortedSetName);
      checkIndexRangeValid(startRank, endRank);

      return sendSortedSetFetchByRank(cacheName, convert(sortedSetName), startRank, endRank);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByScore(
      String cacheName,
      String sortedSetName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable Integer offset,
      @Nullable Integer count) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(sortedSetName);
      checkScoreRangeValid(minScore, maxScore);
      checkSortedSetOffsetValid(offset);
      checkSortedSetCountValid(count);

      return sendSortedSetFetchByScore(
          cacheName, convert(sortedSetName), minScore, maxScore, offset, count);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBack(
      String cacheName,
      String listName,
      List<String> values,
      int truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(values);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListConcatenateBack(
          cacheName, convert(listName), convertStringList(values), ttl, truncateFrontToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBackByteArray(
      String cacheName,
      String listName,
      List<byte[]> values,
      int truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(values);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListConcatenateBack(
          cacheName, convert(listName), convertByteArrayList(values), ttl, truncateFrontToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFront(
      String cacheName,
      String listName,
      List<String> values,
      int truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(values);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListConcatenateFront(
          cacheName, convert(listName), convertStringList(values), ttl, truncateBackToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFrontByteArray(
      String cacheName,
      String listName,
      List<byte[]> values,
      int truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(values);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListConcatenateFront(
          cacheName, convert(listName), convertByteArrayList(values), ttl, truncateBackToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListFetchResponse> listFetch(
      String cacheName, String listName, Integer startIndex, Integer endIndex) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      checkIndexRangeValid(startIndex, endIndex);
      return sendListFetch(cacheName, convert(listName), startIndex, endIndex);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListLengthResponse> listLength(String cacheName, String listName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      return sendListLength(cacheName, convert(listName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListLengthResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPopBackResponse> listPopBack(String cacheName, String listName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      return sendListPopBack(cacheName, convert(listName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPopBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPopFrontResponse> listPopFront(String cacheName, String listName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      return sendListPopFront(cacheName, convert(listName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPopFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPushBackResponse> listPushBack(
      String cacheName,
      String listName,
      String value,
      int truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListPushBack(
          cacheName, convert(listName), convert(value), ttl, truncateFrontToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPushBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPushBackResponse> listPushBack(
      String cacheName,
      String listName,
      byte[] value,
      int truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListPushBack(
          cacheName, convert(listName), convert(value), ttl, truncateFrontToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPushBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPushFrontResponse> listPushFront(
      String cacheName,
      String listName,
      String value,
      int truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListPushFront(
          cacheName, convert(listName), convert(value), ttl, truncateBackToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPushFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPushFrontResponse> listPushFront(
      String cacheName,
      String listName,
      byte[] value,
      int truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListPushFront(
          cacheName, convert(listName), convert(value), ttl, truncateBackToSize);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPushFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListRemoveValueResponse> listRemoveValue(
      String cacheName, String listName, String value) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);

      return sendListRemoveValue(cacheName, convert(listName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListRemoveValueResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListRemoveValueResponse> listRemoveValue(
      String cacheName, String listName, byte[] value) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);

      return sendListRemoveValue(cacheName, convert(listName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListRemoveValueResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListRetainResponse> listRetain(
      String cacheName, String listName, Integer startIndex, Integer endIndex) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      checkIndexRangeValid(startIndex, endIndex);

      return sendListRetain(cacheName, convert(listName), startIndex, endIndex);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListRetainResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionaryFetchResponse> dictionaryFetch(
      String cacheName, String dictionaryName) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);

      return sendDictionaryFetch(cacheName, convert(dictionaryName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, String field, String value, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionarySetField(
          cacheName, convert(dictionaryName), convert(field), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionarySetFieldResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, String field, byte[] value, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionarySetField(
          cacheName, convert(dictionaryName), convert(field), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionarySetFieldResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, byte[] field, String value, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionarySetField(
          cacheName, convert(dictionaryName), convert(field), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionarySetFieldResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionarySetFieldResponse> dictionarySetField(
      String cacheName, String dictionaryName, byte[] field, byte[] value, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionarySetField(
          cacheName, convert(dictionaryName), convert(field), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionarySetFieldResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFields(
      String cacheName, String dictionaryName, Map<String, String> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidValue(elements);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionarySetFields(
          cacheName, convert(dictionaryName), convertStringStringEntryList(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionarySetFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsStringBytes(
      String cacheName, String dictionaryName, Map<String, byte[]> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidValue(elements);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionarySetFields(
          cacheName, convert(dictionaryName), convertStringBytesEntryList(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionarySetFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsBytesString(
      String cacheName, String dictionaryName, Map<byte[], String> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidValue(elements);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionarySetFields(
          cacheName, convert(dictionaryName), convertBytesStringEntryList(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionarySetFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionarySetFieldsResponse> dictionarySetFieldsBytesBytes(
      String cacheName, String dictionaryName, Map<byte[], byte[]> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidValue(elements);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionarySetFields(
          cacheName, convert(dictionaryName), convertBytesBytesEntryList(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionarySetFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionaryGetFieldResponse> dictionaryGetField(
      String cacheName, String dictionaryName, String field) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);

      return sendDictionaryGetField(cacheName, convert(dictionaryName), convert(field));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryGetFieldResponse.Error(
              CacheServiceExceptionMapper.convert(e), convert(field)));
    }
  }

  CompletableFuture<CacheDictionaryGetFieldResponse> dictionaryGetField(
      String cacheName, String dictionaryName, byte[] field) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);

      return sendDictionaryGetField(cacheName, convert(dictionaryName), convert(field));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryGetFieldResponse.Error(
              CacheServiceExceptionMapper.convert(e), convert(field)));
    }
  }

  CompletableFuture<CacheDictionaryGetFieldsResponse> dictionaryGetFields(
      String cacheName, String dictionaryName, List<String> fields) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(fields);
      for (String field : fields) {
        ensureValidKey(field);
      }

      return sendDictionaryGetFields(cacheName, convert(dictionaryName), convertStringList(fields));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryGetFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionaryGetFieldsResponse> dictionaryGetFieldsByteArray(
      String cacheName, String dictionaryName, List<byte[]> fields) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(fields);
      for (byte[] field : fields) {
        ensureValidKey(field);
      }

      return sendDictionaryGetFields(
          cacheName, convert(dictionaryName), convertByteArrayList(fields));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryGetFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionaryIncrementResponse> dictionaryIncrement(
      String cacheName, String dictionaryName, String field, long amount, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);
      ensureValidValue(amount);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionaryIncrement(
          cacheName, convert(dictionaryName), convert(field), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryIncrementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionaryIncrementResponse> dictionaryIncrement(
      String cacheName, String dictionaryName, byte[] field, long amount, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);
      ensureValidValue(amount);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendDictionaryIncrement(
          cacheName, convert(dictionaryName), convert(field), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryIncrementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionaryRemoveFieldResponse> dictionaryRemoveField(
      String cacheName, String dictionaryName, String field) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);

      return sendDictionaryRemoveField(cacheName, convert(dictionaryName), convert(field));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryRemoveFieldResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionaryRemoveFieldResponse> dictionaryRemoveField(
      String cacheName, String dictionaryName, byte[] field) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);

      return sendDictionaryRemoveField(cacheName, convert(dictionaryName), convert(field));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryRemoveFieldResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionaryRemoveFieldsResponse> dictionaryRemoveFields(
      String cacheName, String dictionaryName, List<String> fields) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(fields);
      for (String field : fields) {
        ensureValidKey(field);
      }

      return sendDictionaryRemoveFields(
          cacheName, convert(dictionaryName), convertStringList(fields));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryRemoveFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheDictionaryRemoveFieldsResponse> dictionaryRemoveFieldsByteArray(
      String cacheName, String dictionaryName, List<byte[]> fields) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(fields);
      for (byte[] field : fields) {
        ensureValidKey(field);
      }

      return sendDictionaryRemoveFields(
          cacheName, convert(dictionaryName), convertByteArrayList(fields));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheDictionaryRemoveFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  private ByteString convert(String stringToEncode) {
    if (stringToEncode == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFromUtf8(stringToEncode);
  }

  private ByteString convert(byte[] bytes) {
    if (bytes == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(bytes);
  }

  private ByteString convert(ByteBuffer byteBuffer) {
    return ByteString.copyFrom(byteBuffer);
  }

  private Set<ByteString> convertStringSet(Set<String> strings) {
    return strings.stream().map(this::convert).collect(Collectors.toSet());
  }

  private Set<ByteString> convertByteArraySet(Set<byte[]> strings) {
    return strings.stream().map(this::convert).collect(Collectors.toSet());
  }

  private List<ByteString> convertStringList(List<String> strings) {
    return strings.stream().map(this::convert).collect(Collectors.toList());
  }

  private List<ByteString> convertByteArrayList(List<byte[]> byteArrays) {
    return byteArrays.stream().map(this::convert).collect(Collectors.toList());
  }

  private Map<ByteString, ByteString> convertStringStringEntryList(Map<String, String> elements) {
    return elements.entrySet().stream()
        .collect(
            Collectors.toMap(entry -> convert(entry.getKey()), entry -> convert(entry.getValue())));
  }

  private Map<ByteString, ByteString> convertStringBytesEntryList(Map<String, byte[]> elements) {
    return elements.entrySet().stream()
        .collect(
            Collectors.toMap(entry -> convert(entry.getKey()), entry -> convert(entry.getValue())));
  }

  private Map<ByteString, ByteString> convertBytesStringEntryList(Map<byte[], String> elements) {
    return elements.entrySet().stream()
        .collect(
            Collectors.toMap(entry -> convert(entry.getKey()), entry -> convert(entry.getValue())));
  }

  private Map<ByteString, ByteString> convertBytesBytesEntryList(Map<byte[], byte[]> elements) {
    return elements.entrySet().stream()
        .collect(
            Collectors.toMap(entry -> convert(entry.getKey()), entry -> convert(entry.getValue())));
  }

  private Map<ByteString, Double> convertStringScoreMap(Map<String, Double> elements) {
    return elements.entrySet().stream()
        .collect(Collectors.toMap(entry -> convert(entry.getKey()), Map.Entry::getValue));
  }

  private Map<ByteString, Double> convertBytesScoreMap(Map<byte[], Double> elements) {
    return elements.entrySet().stream()
        .collect(Collectors.toMap(entry -> convert(entry.getKey()), Map.Entry::getValue));
  }

  private CompletableFuture<CacheGetResponse> sendGet(String cacheName, ByteString key) {
    checkCacheNameValid(cacheName);

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_GetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata).get(buildGetRequest(key));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheGetResponse> returnFuture =
        new CompletableFuture<CacheGetResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_GetResponse>() {
          @Override
          public void onSuccess(_GetResponse rsp) {
            final ECacheResult result = rsp.getResult();

            final CacheGetResponse response;
            if (result == ECacheResult.Hit) {
              response = new CacheGetResponse.Hit(rsp.getCacheBody());
            } else if (result == ECacheResult.Miss) {
              response = new CacheGetResponse.Miss();
            } else {
              response =
                  new CacheGetResponse.Error(
                      new InternalServerException("Unsupported cache Get result: " + result));
            }
            returnFuture.complete(response);
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheGetResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheDeleteResponse> sendDelete(String cacheName, ByteString key) {
    checkCacheNameValid(cacheName);
    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DeleteResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata).delete(buildDeleteRequest(key));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDeleteResponse> returnFuture =
        new CompletableFuture<CacheDeleteResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DeleteResponse>() {
          @Override
          public void onSuccess(_DeleteResponse rsp) {
            returnFuture.complete(new CacheDeleteResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheDeleteResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSetResponse> sendSet(
      String cacheName, ByteString key, ByteString value, Duration ttl) {
    checkCacheNameValid(cacheName);

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .set(buildSetRequest(key, value, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetResponse> returnFuture =
        new CompletableFuture<CacheSetResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SetResponse>() {
          @Override
          public void onSuccess(_SetResponse rsp) {
            returnFuture.complete(new CacheSetResponse.Success(value));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheIncrementResponse> sendIncrement(
      String cacheName, ByteString field, long amount, Duration ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_IncrementResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .increment(buildIncrementRequest(field, amount, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheIncrementResponse> returnFuture =
        new CompletableFuture<CacheIncrementResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_IncrementResponse>() {
          @Override
          public void onSuccess(_IncrementResponse rsp) {
            returnFuture.complete(new CacheIncrementResponse.Success((int) rsp.getValue()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheIncrementResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheSetIfNotExistsResponse> sendSetIfNotExists(
      String cacheName, ByteString key, ByteString value, Duration ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetIfNotExistsResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setIfNotExists(buildSetIfNotExistsRequest(key, value, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetIfNotExistsResponse> returnFuture =
        new CompletableFuture<CacheSetIfNotExistsResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SetIfNotExistsResponse>() {
          @Override
          public void onSuccess(_SetIfNotExistsResponse rsp) {
            if (rsp.getResultCase().equals(_SetIfNotExistsResponse.ResultCase.STORED)) {
              returnFuture.complete(new CacheSetIfNotExistsResponse.Stored(key, value));
            } else if (rsp.getResultCase().equals(_SetIfNotExistsResponse.ResultCase.NOT_STORED)) {
              returnFuture.complete(new CacheSetIfNotExistsResponse.NotStored());
            }
          }

          @Override
          public void onFailure(Throwable e) {
            returnFuture.complete(
                new CacheSetIfNotExistsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheSetAddElementResponse> sendSetAddElement(
      String cacheName, ByteString setName, ByteString element, CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetUnionResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setUnion(buildSetUnionRequest(setName, Collections.singleton(element), ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetAddElementResponse> returnFuture =
        new CompletableFuture<CacheSetAddElementResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SetUnionResponse>() {
          @Override
          public void onSuccess(_SetUnionResponse rsp) {
            returnFuture.complete(new CacheSetAddElementResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetAddElementResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSetAddElementsResponse> sendSetAddElements(
      String cacheName, ByteString setName, Set<ByteString> elements, CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetUnionResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setUnion(buildSetUnionRequest(setName, elements, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetAddElementsResponse> returnFuture =
        new CompletableFuture<CacheSetAddElementsResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SetUnionResponse>() {
          @Override
          public void onSuccess(_SetUnionResponse rsp) {
            returnFuture.complete(new CacheSetAddElementsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetAddElementsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSetRemoveElementResponse> sendSetRemoveElement(
      String cacheName, ByteString setName, ByteString element) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetDifferenceResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setDifference(buildSetDifferenceRequest(setName, Collections.singleton(element)));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetRemoveElementResponse> returnFuture =
        new CompletableFuture<CacheSetRemoveElementResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SetDifferenceResponse>() {
          @Override
          public void onSuccess(_SetDifferenceResponse rsp) {
            returnFuture.complete(new CacheSetRemoveElementResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetRemoveElementResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSetRemoveElementsResponse> sendSetRemoveElements(
      String cacheName, ByteString setName, Set<ByteString> elements) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetDifferenceResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setDifference(buildSetDifferenceRequest(setName, elements));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetRemoveElementsResponse> returnFuture =
        new CompletableFuture<CacheSetRemoveElementsResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SetDifferenceResponse>() {
          @Override
          public void onSuccess(_SetDifferenceResponse rsp) {
            returnFuture.complete(new CacheSetRemoveElementsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetRemoveElementsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSetFetchResponse> sendSetFetch(
      String cacheName, ByteString setName) {
    checkCacheNameValid(cacheName);

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setFetch(buildSetFetchRequest(setName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSetFetchResponse> returnFuture =
        new CompletableFuture<CacheSetFetchResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SetFetchResponse>() {
          @Override
          public void onSuccess(_SetFetchResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(
                  new CacheSetFetchResponse.Hit(rsp.getFound().getElementsList()));
            } else {
              returnFuture.complete(new CacheSetFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSortedSetPutElementResponse> sendSortedSetPutElement(
      String cacheName,
      ByteString setName,
      ByteString element,
      double score,
      CollectionTtl collectionTtl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SortedSetPutResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .sortedSetPut(
                buildSortedSetPutRequest(
                    setName, Collections.singletonMap(element, score), collectionTtl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSortedSetPutElementResponse> returnFuture =
        new CompletableFuture<CacheSortedSetPutElementResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SortedSetPutResponse>() {
          @Override
          public void onSuccess(_SortedSetPutResponse rsp) {
            returnFuture.complete(new CacheSortedSetPutElementResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSortedSetPutElementResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSortedSetPutElementsResponse> sendSortedSetPutElements(
      String cacheName,
      ByteString setName,
      Map<ByteString, Double> elements,
      CollectionTtl collectionTtl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SortedSetPutResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .sortedSetPut(buildSortedSetPutRequest(setName, elements, collectionTtl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSortedSetPutElementsResponse> returnFuture =
        new CompletableFuture<CacheSortedSetPutElementsResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SortedSetPutResponse>() {
          @Override
          public void onSuccess(_SortedSetPutResponse rsp) {
            returnFuture.complete(new CacheSortedSetPutElementsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSortedSetPutElementsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSortedSetFetchResponse> sendSortedSetFetchByRank(
      String cacheName,
      ByteString setName,
      @Nullable Integer startRank,
      @Nullable Integer endRank) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SortedSetFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .sortedSetFetch(buildSortedSetFetchRequestByRank(setName, startRank, endRank));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSortedSetFetchResponse> returnFuture =
        new CompletableFuture<CacheSortedSetFetchResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SortedSetFetchResponse>() {
          @Override
          public void onSuccess(_SortedSetFetchResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(
                  new CacheSortedSetFetchResponse.Hit(
                      rsp.getFound().getValuesWithScores().getElementsList()));
            } else {
              returnFuture.complete(new CacheSortedSetFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSortedSetFetchResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheSortedSetFetchResponse> sendSortedSetFetchByScore(
      String cacheName,
      ByteString setName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable Integer offset,
      @Nullable Integer count) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SortedSetFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .sortedSetFetch(
                buildSortedSetFetchRequestByScore(setName, minScore, maxScore, offset, count));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheSortedSetFetchResponse> returnFuture =
        new CompletableFuture<CacheSortedSetFetchResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_SortedSetFetchResponse>() {
          @Override
          public void onSuccess(_SortedSetFetchResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(
                  new CacheSortedSetFetchResponse.Hit(
                      rsp.getFound().getValuesWithScores().getElementsList()));
            } else {
              returnFuture.complete(new CacheSortedSetFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheSortedSetFetchResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheListConcatenateBackResponse> sendListConcatenateBack(
      String cacheName,
      ByteString listName,
      List<ByteString> values,
      CollectionTtl ttl,
      int truncateFrontToSize) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListConcatenateBackResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listConcatenateBack(
                buildListConcatenateBackRequest(listName, values, ttl, truncateFrontToSize));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListConcatenateBackResponse> returnFuture =
        new CompletableFuture<CacheListConcatenateBackResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListConcatenateBackResponse>() {
          @Override
          public void onSuccess(_ListConcatenateBackResponse rsp) {
            returnFuture.complete(
                new CacheListConcatenateBackResponse.Success(rsp.getListLength()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListConcatenateBackResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheListConcatenateFrontResponse> sendListConcatenateFront(
      String cacheName,
      ByteString listName,
      List<ByteString> values,
      CollectionTtl ttl,
      int truncateBackToSize) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListConcatenateFrontResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listConcatenateFront(
                buildListConcatenateFrontRequest(listName, values, ttl, truncateBackToSize));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListConcatenateFrontResponse> returnFuture =
        new CompletableFuture<CacheListConcatenateFrontResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListConcatenateFrontResponse>() {
          @Override
          public void onSuccess(_ListConcatenateFrontResponse rsp) {
            returnFuture.complete(
                new CacheListConcatenateFrontResponse.Success(rsp.getListLength()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListConcatenateFrontResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheListFetchResponse> sendListFetch(
      String cacheName, ByteString listName, Integer startIndex, Integer endIndex) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listFetch(buildListFetchRequest(listName, startIndex, endIndex));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListFetchResponse> returnFuture =
        new CompletableFuture<CacheListFetchResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListFetchResponse>() {
          @Override
          public void onSuccess(_ListFetchResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(new CacheListFetchResponse.Hit(rsp.getFound().getValuesList()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new CacheListFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListFetchResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheListLengthResponse> sendListLength(
      String cacheName, ByteString listName) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListLengthResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listLength(buildListLengthRequest(listName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListLengthResponse> returnFuture =
        new CompletableFuture<CacheListLengthResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListLengthResponse>() {
          @Override
          public void onSuccess(_ListLengthResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(new CacheListLengthResponse.Hit(rsp.getFound().getLength()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new CacheListLengthResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListLengthResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheListPopBackResponse> sendListPopBack(
      String cacheName, ByteString listName) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListPopBackResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listPopBack(buildListPopBackRequest(listName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListPopBackResponse> returnFuture =
        new CompletableFuture<CacheListPopBackResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListPopBackResponse>() {
          @Override
          public void onSuccess(_ListPopBackResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(new CacheListPopBackResponse.Hit(rsp.getFound().getBack()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new CacheListPopBackResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListPopBackResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheListPushBackResponse> sendListPushBack(
      String cacheName,
      ByteString listName,
      ByteString value,
      CollectionTtl ttl,
      int truncateFrontToSize) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListPushBackResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listPushBack(buildListPushBackRequest(listName, value, ttl, truncateFrontToSize));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListPushBackResponse> returnFuture =
        new CompletableFuture<CacheListPushBackResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListPushBackResponse>() {
          @Override
          public void onSuccess(_ListPushBackResponse rsp) {
            returnFuture.complete(new CacheListPushBackResponse.Success(rsp.getListLength()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListPushBackResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheListPopFrontResponse> sendListPopFront(
      String cacheName, ByteString listName) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListPopFrontResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listPopFront(buildListPopFrontRequest(listName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListPopFrontResponse> returnFuture =
        new CompletableFuture<CacheListPopFrontResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListPopFrontResponse>() {
          @Override
          public void onSuccess(_ListPopFrontResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(new CacheListPopFrontResponse.Hit(rsp.getFound().getFront()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new CacheListPopFrontResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListPopFrontResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<CacheListPushFrontResponse> sendListPushFront(
      String cacheName,
      ByteString listName,
      ByteString value,
      CollectionTtl ttl,
      int truncateBackToSize) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListPushFrontResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listPushFront(buildListPushFrontRequest(listName, value, ttl, truncateBackToSize));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListPushFrontResponse> returnFuture =
        new CompletableFuture<CacheListPushFrontResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListPushFrontResponse>() {
          @Override
          public void onSuccess(_ListPushFrontResponse rsp) {
            returnFuture.complete(new CacheListPushFrontResponse.Success(rsp.getListLength()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListPushFrontResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheListRemoveValueResponse> sendListRemoveValue(
      String cacheName, ByteString listName, ByteString value) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListRemoveResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listRemove(buildListRemoveValueRequest(listName, value));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListRemoveValueResponse> returnFuture =
        new CompletableFuture<CacheListRemoveValueResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListRemoveResponse>() {
          @Override
          public void onSuccess(_ListRemoveResponse rsp) {
            returnFuture.complete(new CacheListRemoveValueResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListRemoveValueResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheListRetainResponse> sendListRetain(
      String cacheName, ByteString listName, Integer startIndex, Integer endIndex) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListRetainResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listRetain(buildListRetainRequest(listName, startIndex, endIndex));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheListRetainResponse> returnFuture =
        new CompletableFuture<CacheListRetainResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_ListRetainResponse>() {
          @Override
          public void onSuccess(_ListRetainResponse rsp) {
            returnFuture.complete(new CacheListRetainResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheListRetainResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheDictionaryFetchResponse> sendDictionaryFetch(
      String cacheName, ByteString dictionaryName) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryFetch(buildDictionaryFetchRequest(dictionaryName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDictionaryFetchResponse> returnFuture =
        new CompletableFuture<CacheDictionaryFetchResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DictionaryFetchResponse>() {
          @Override
          public void onSuccess(_DictionaryFetchResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(
                  new CacheDictionaryFetchResponse.Hit(rsp.getFound().getItemsList()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new CacheDictionaryFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheDictionaryFetchResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheDictionarySetFieldResponse> sendDictionarySetField(
      String cacheName,
      ByteString dictionaryName,
      ByteString field,
      ByteString value,
      CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionarySetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionarySet(buildDictionarySetFieldRequest(dictionaryName, field, value, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDictionarySetFieldResponse> returnFuture =
        new CompletableFuture<CacheDictionarySetFieldResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DictionarySetResponse>() {
          @Override
          public void onSuccess(_DictionarySetResponse rsp) {
            returnFuture.complete(new CacheDictionarySetFieldResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheDictionarySetFieldResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheDictionarySetFieldsResponse> sendDictionarySetFields(
      String cacheName,
      ByteString dictionaryName,
      Map<ByteString, ByteString> elements,
      CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionarySetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionarySet(buildDictionarySetFieldsRequest(dictionaryName, elements, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDictionarySetFieldsResponse> returnFuture =
        new CompletableFuture<CacheDictionarySetFieldsResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DictionarySetResponse>() {
          @Override
          public void onSuccess(_DictionarySetResponse rsp) {
            returnFuture.complete(new CacheDictionarySetFieldsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheDictionarySetFieldsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheDictionaryGetFieldResponse> sendDictionaryGetField(
      String cacheName, ByteString dictionaryName, ByteString field) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryGetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryGet(buildDictionaryGetFieldRequest(dictionaryName, field));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDictionaryGetFieldResponse> returnFuture =
        new CompletableFuture<CacheDictionaryGetFieldResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DictionaryGetResponse>() {
          @Override
          public void onSuccess(_DictionaryGetResponse rsp) {
            if (rsp.hasMissing()) {
              returnFuture.complete(new CacheDictionaryGetFieldResponse.Miss(field));
            } else if (rsp.hasFound()) {
              if (rsp.getFound().getItemsList().size() == 0) {
                returnFuture.complete(
                    new CacheDictionaryGetFieldResponse.Error(
                        CacheServiceExceptionMapper.convert(
                            new Exception(
                                "_DictionaryGetResponseResponse contained no data but was found"),
                            metadata),
                        field));
              } else if (rsp.getFound().getItemsList().get(0).getResult() == ECacheResult.Miss) {
                returnFuture.complete(new CacheDictionaryGetFieldResponse.Miss(field));
              } else {
                returnFuture.complete(
                    new CacheDictionaryGetFieldResponse.Hit(
                        field, rsp.getFound().getItemsList().get(0).getCacheBody()));
              }
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheDictionaryGetFieldResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata), field));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheDictionaryGetFieldsResponse> sendDictionaryGetFields(
      String cacheName, ByteString dictionaryName, List<ByteString> fields) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryGetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryGet(buildDictionaryGetFieldsRequest(dictionaryName, fields));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDictionaryGetFieldsResponse> returnFuture =
        new CompletableFuture<CacheDictionaryGetFieldsResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DictionaryGetResponse>() {
          @Override
          public void onSuccess(_DictionaryGetResponse rsp) {
            if (rsp.hasFound()) {
              returnFuture.complete(
                  new CacheDictionaryGetFieldsResponse.Hit(fields, rsp.getFound().getItemsList()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new CacheDictionaryGetFieldsResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheDictionaryGetFieldsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheDictionaryIncrementResponse> sendDictionaryIncrement(
      String cacheName,
      ByteString dictionaryName,
      ByteString field,
      long amount,
      CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryIncrementResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryIncrement(
                buildDictionaryIncrementRequest(dictionaryName, field, amount, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDictionaryIncrementResponse> returnFuture =
        new CompletableFuture<CacheDictionaryIncrementResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DictionaryIncrementResponse>() {
          @Override
          public void onSuccess(_DictionaryIncrementResponse rsp) {
            returnFuture.complete(
                new CacheDictionaryIncrementResponse.Success((int) rsp.getValue()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheDictionaryIncrementResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheDictionaryRemoveFieldResponse> sendDictionaryRemoveField(
      String cacheName, ByteString dictionaryName, ByteString field) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryDeleteResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryDelete(buildDictionaryRemoveFieldRequest(dictionaryName, field));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDictionaryRemoveFieldResponse> returnFuture =
        new CompletableFuture<CacheDictionaryRemoveFieldResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DictionaryDeleteResponse>() {
          @Override
          public void onSuccess(_DictionaryDeleteResponse rsp) {
            returnFuture.complete(new CacheDictionaryRemoveFieldResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheDictionaryRemoveFieldResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<CacheDictionaryRemoveFieldsResponse> sendDictionaryRemoveFields(
      String cacheName, ByteString dictionaryName, List<ByteString> fields) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryDeleteResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryDelete(buildDictionaryRemoveFieldsRequest(dictionaryName, fields));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<CacheDictionaryRemoveFieldsResponse> returnFuture =
        new CompletableFuture<CacheDictionaryRemoveFieldsResponse>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            // propagate cancel to the listenable future if called on returned completable future
            final boolean result = rspFuture.cancel(mayInterruptIfRunning);
            super.cancel(mayInterruptIfRunning);
            return result;
          }
        };

    // Convert returned ListenableFuture to CompletableFuture
    Futures.addCallback(
        rspFuture,
        new FutureCallback<_DictionaryDeleteResponse>() {
          @Override
          public void onSuccess(_DictionaryDeleteResponse rsp) {
            returnFuture.complete(new CacheDictionaryRemoveFieldsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new CacheDictionaryRemoveFieldsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private static Metadata metadataWithCache(String cacheName) {
    final Metadata metadata = new Metadata();
    metadata.put(CACHE_NAME_KEY, cacheName);

    return metadata;
  }

  private static ScsGrpc.ScsFutureStub attachMetadata(
      ScsGrpc.ScsFutureStub stub, Metadata metadata) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  private _GetRequest buildGetRequest(ByteString key) {
    return _GetRequest.newBuilder().setCacheKey(key).build();
  }

  private _DeleteRequest buildDeleteRequest(ByteString key) {
    return _DeleteRequest.newBuilder().setCacheKey(key).build();
  }

  private _SetRequest buildSetRequest(ByteString key, ByteString value, Duration ttl) {
    return _SetRequest.newBuilder()
        .setCacheKey(key)
        .setCacheBody(value)
        .setTtlMilliseconds(ttl.toMillis())
        .build();
  }

  private _IncrementRequest buildIncrementRequest(ByteString field, long amount, Duration ttl) {
    return _IncrementRequest.newBuilder()
        .setCacheKey(field)
        .setAmount(amount)
        .setTtlMilliseconds(ttl.toMillis())
        .build();
  }

  private _SetIfNotExistsRequest buildSetIfNotExistsRequest(
      ByteString key, ByteString value, Duration ttl) {
    return _SetIfNotExistsRequest.newBuilder()
        .setCacheKey(key)
        .setCacheBody(value)
        .setTtlMilliseconds(ttl.toMillis())
        .build();
  }

  private _SetUnionRequest buildSetUnionRequest(
      ByteString setName, Set<ByteString> elements, CollectionTtl ttl) {
    return _SetUnionRequest.newBuilder()
        .setSetName(setName)
        .addAllElements(elements)
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private _SetDifferenceRequest buildSetDifferenceRequest(
      ByteString setName, Set<ByteString> elements) {
    return _SetDifferenceRequest.newBuilder()
        .setSetName(setName)
        .setSubtrahend(
            _SetDifferenceRequest._Subtrahend.newBuilder()
                .setSet(
                    _SetDifferenceRequest._Subtrahend._Set.newBuilder()
                        .addAllElements(elements)
                        .build())
                .build())
        .build();
  }

  private _SetFetchRequest buildSetFetchRequest(ByteString setName) {
    return _SetFetchRequest.newBuilder().setSetName(setName).build();
  }

  private _SortedSetPutRequest buildSortedSetPutRequest(
      ByteString sortedSetName, Map<ByteString, Double> elements, CollectionTtl ttl) {
    return _SortedSetPutRequest.newBuilder()
        .setSetName(sortedSetName)
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .addAllElements(
            elements.entrySet().stream()
                .map(
                    e ->
                        _SortedSetElement.newBuilder()
                            .setValue(e.getKey())
                            .setScore(e.getValue())
                            .build())
                .collect(Collectors.toSet()))
        .build();
  }

  private _SortedSetFetchRequest buildSortedSetFetchRequestByRank(
      ByteString sortedSetName, @Nullable Integer startRank, @Nullable Integer endRank) {

    final _SortedSetFetchRequest._ByIndex.Builder indexBuilder =
        _SortedSetFetchRequest._ByIndex.newBuilder();
    if (startRank != null) {
      indexBuilder.setInclusiveStartIndex(startRank);
    } else {
      indexBuilder.setUnboundedStart(_Unbounded.newBuilder());
    }
    if (endRank != null) {
      indexBuilder.setExclusiveEndIndex(endRank);
    } else {
      indexBuilder.setUnboundedEnd(_Unbounded.newBuilder());
    }

    return _SortedSetFetchRequest.newBuilder()
        .setSetName(sortedSetName)
        .setWithScores(true)
        .setOrder(_SortedSetFetchRequest.Order.ASCENDING)
        .setByIndex(indexBuilder)
        .build();
  }

  private _SortedSetFetchRequest buildSortedSetFetchRequestByScore(
      ByteString sortedSetName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable Integer offset,
      @Nullable Integer count) {

    final _SortedSetFetchRequest._ByScore.Builder scoreBuilder =
        _SortedSetFetchRequest._ByScore.newBuilder();
    if (minScore != null) {
      scoreBuilder.setMinScore(
          _SortedSetFetchRequest._ByScore._Score.newBuilder().setScore(minScore));
    } else {
      scoreBuilder.setUnboundedMin(_Unbounded.newBuilder());
    }
    if (maxScore != null) {
      scoreBuilder.setMaxScore(
          _SortedSetFetchRequest._ByScore._Score.newBuilder().setScore(maxScore));
    } else {
      scoreBuilder.setUnboundedMax(_Unbounded.newBuilder());
    }
    if (offset != null) {
      scoreBuilder.setOffset(offset);
    } else {
      scoreBuilder.setOffset(0);
    }
    if (count != null) {
      scoreBuilder.setCount(count);
    } else {
      scoreBuilder.setCount(-1);
    }

    return _SortedSetFetchRequest.newBuilder()
        .setSetName(sortedSetName)
        .setWithScores(true)
        .setOrder(_SortedSetFetchRequest.Order.ASCENDING)
        .setByScore(scoreBuilder)
        .build();
  }

  private _ListConcatenateBackRequest buildListConcatenateBackRequest(
      ByteString listName, List<ByteString> values, CollectionTtl ttl, int truncateFrontToSize) {
    _ListConcatenateBackRequest request =
        _ListConcatenateBackRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .setTruncateFrontToSize(truncateFrontToSize)
            .addAllValues(values)
            .build();
    return request;
  }

  private _ListConcatenateFrontRequest buildListConcatenateFrontRequest(
      ByteString listName, List<ByteString> values, CollectionTtl ttl, int truncateBackToSize) {
    _ListConcatenateFrontRequest request =
        _ListConcatenateFrontRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .setTruncateBackToSize(truncateBackToSize)
            .addAllValues(values)
            .build();
    return request;
  }

  private _ListFetchRequest buildListFetchRequest(
      ByteString listName, Integer startIndex, Integer endIndex) {
    _ListFetchRequest request;
    if (startIndex != null && endIndex != null) {
      request =
          _ListFetchRequest.newBuilder()
              .setListName(listName)
              .setInclusiveStart(startIndex)
              .setExclusiveEnd(endIndex)
              .build();
    } else if (startIndex != null && endIndex == null) {
      request =
          _ListFetchRequest.newBuilder()
              .setListName(listName)
              .setInclusiveStart(startIndex)
              .setUnboundedEnd(_Unbounded.newBuilder().build())
              .build();
    } else if (startIndex == null && endIndex != null) {
      request =
          _ListFetchRequest.newBuilder()
              .setListName(listName)
              .setUnboundedStart(_Unbounded.newBuilder().build())
              .setExclusiveEnd(endIndex)
              .build();
    } else {
      request =
          _ListFetchRequest.newBuilder()
              .setListName(listName)
              .setUnboundedStart(_Unbounded.newBuilder().build())
              .setUnboundedEnd(_Unbounded.newBuilder().build())
              .build();
    }

    return request;
  }

  private _ListLengthRequest buildListLengthRequest(ByteString listName) {
    return _ListLengthRequest.newBuilder().setListName(listName).build();
  }

  private _ListPopBackRequest buildListPopBackRequest(ByteString listName) {
    return _ListPopBackRequest.newBuilder().setListName(listName).build();
  }

  private _ListPopFrontRequest buildListPopFrontRequest(ByteString listName) {
    return _ListPopFrontRequest.newBuilder().setListName(listName).build();
  }

  private _ListPushBackRequest buildListPushBackRequest(
      ByteString listName, ByteString value, CollectionTtl ttl, int truncateFrontToSize) {
    _ListPushBackRequest request =
        _ListPushBackRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .setTruncateFrontToSize(truncateFrontToSize)
            .setValue(value)
            .build();
    return request;
  }

  private _ListPushFrontRequest buildListPushFrontRequest(
      ByteString listName, ByteString value, CollectionTtl ttl, int truncateBackToSize) {
    _ListPushFrontRequest request =
        _ListPushFrontRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .setTruncateBackToSize(truncateBackToSize)
            .setValue(value)
            .build();
    return request;
  }

  private _ListRemoveRequest buildListRemoveValueRequest(ByteString listName, ByteString value) {
    _ListRemoveRequest request =
        _ListRemoveRequest.newBuilder()
            .setListName(listName)
            .setAllElementsWithValue(value)
            .build();
    return request;
  }

  private _ListRetainRequest buildListRetainRequest(
      ByteString listName, Integer startIndex, Integer endIndex) {
    _ListRetainRequest request;
    if (startIndex != null && endIndex != null) {
      request =
          _ListRetainRequest.newBuilder()
              .setListName(listName)
              .setInclusiveStart(startIndex)
              .setExclusiveEnd(endIndex)
              .build();
    } else if (startIndex != null && endIndex == null) {
      request =
          _ListRetainRequest.newBuilder()
              .setListName(listName)
              .setInclusiveStart(startIndex)
              .setUnboundedEnd(_Unbounded.newBuilder().build())
              .build();
    } else if (startIndex == null && endIndex != null) {
      request =
          _ListRetainRequest.newBuilder()
              .setListName(listName)
              .setUnboundedStart(_Unbounded.newBuilder().build())
              .setExclusiveEnd(endIndex)
              .build();
    } else {
      request =
          _ListRetainRequest.newBuilder()
              .setListName(listName)
              .setUnboundedStart(_Unbounded.newBuilder().build())
              .setUnboundedEnd(_Unbounded.newBuilder().build())
              .build();
    }

    return request;
  }

  private _DictionaryFetchRequest buildDictionaryFetchRequest(ByteString dictionaryName) {
    return _DictionaryFetchRequest.newBuilder().setDictionaryName(dictionaryName).build();
  }

  private _DictionarySetRequest buildDictionarySetFieldRequest(
      ByteString dictionaryName, ByteString field, ByteString value, CollectionTtl ttl) {
    return _DictionarySetRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .addItems(toSingletonFieldValuePair(field, value))
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private _DictionaryFieldValuePair toSingletonFieldValuePair(ByteString field, ByteString value) {
    _DictionaryFieldValuePair dictionaryFieldValuePair =
        _DictionaryFieldValuePair.newBuilder().setField(field).setValue(value).build();

    return dictionaryFieldValuePair;
  }

  private _DictionarySetRequest buildDictionarySetFieldsRequest(
      ByteString dictionaryName, Map<ByteString, ByteString> elements, CollectionTtl ttl) {
    return _DictionarySetRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .addAllItems(toDictionaryFieldValuePairs(elements))
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private List<_DictionaryFieldValuePair> toDictionaryFieldValuePairs(
      Map<ByteString, ByteString> fieldValuepairs) {
    return fieldValuepairs.entrySet().stream()
        .map(
            fieldValuePair ->
                _DictionaryFieldValuePair.newBuilder()
                    .setField(fieldValuePair.getKey())
                    .setValue(fieldValuePair.getValue())
                    .build())
        .collect(Collectors.toList());
  }

  private _DictionaryGetRequest buildDictionaryGetFieldRequest(
      ByteString dictionaryName, ByteString field) {
    return _DictionaryGetRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .addFields(field)
        .build();
  }

  private _DictionaryGetRequest buildDictionaryGetFieldsRequest(
      ByteString dictionaryName, List<ByteString> fields) {
    return _DictionaryGetRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .addAllFields(fields)
        .build();
  }

  private _DictionaryIncrementRequest buildDictionaryIncrementRequest(
      ByteString dictionaryName, ByteString field, long amount, CollectionTtl ttl) {
    return _DictionaryIncrementRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .setField(field)
        .setAmount(amount)
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private _DictionaryDeleteRequest buildDictionaryRemoveFieldRequest(
      ByteString dictionaryName, ByteString field) {
    return _DictionaryDeleteRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .setSome(addSomeFieldsToRemove(field))
        .build();
  }

  private _DictionaryDeleteRequest.Some addSomeFieldsToRemove(ByteString field) {
    return _DictionaryDeleteRequest.Some.newBuilder().addFields(field).build();
  }

  private _DictionaryDeleteRequest buildDictionaryRemoveFieldsRequest(
      ByteString dictionaryName, List<ByteString> fields) {
    return _DictionaryDeleteRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .setSome(addSomeFieldsToRemove(fields))
        .build();
  }

  private _DictionaryDeleteRequest.Some addSomeFieldsToRemove(List<ByteString> fields) {
    return _DictionaryDeleteRequest.Some.newBuilder().addAllFields(fields).build();
  }

  @Override
  public void close() {
    scsDataGrpcStubsManager.close();
  }
}
