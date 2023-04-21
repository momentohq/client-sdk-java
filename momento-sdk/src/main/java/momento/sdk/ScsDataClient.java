package momento.sdk;

import static momento.sdk.ValidationUtils.checkCacheNameValid;
import static momento.sdk.ValidationUtils.checkDictionaryNameValid;
import static momento.sdk.ValidationUtils.checkIndexRangeValid;
import static momento.sdk.ValidationUtils.checkListNameValid;
import static momento.sdk.ValidationUtils.checkScoreRangeValid;
import static momento.sdk.ValidationUtils.checkSetNameValid;
import static momento.sdk.ValidationUtils.checkSortedSetCountValid;
import static momento.sdk.ValidationUtils.checkSortedSetNameValid;
import static momento.sdk.ValidationUtils.checkSortedSetOffsetValid;
import static momento.sdk.ValidationUtils.ensureValidCacheSet;
import static momento.sdk.ValidationUtils.ensureValidKey;
import static momento.sdk.ValidationUtils.ensureValidTruncateToSize;
import static momento.sdk.ValidationUtils.ensureValidValue;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import grpc.cache_client.ECacheResult;
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
import grpc.cache_client._SortedSetGetRankRequest;
import grpc.cache_client._SortedSetGetRankResponse;
import grpc.cache_client._SortedSetGetScoreRequest;
import grpc.cache_client._SortedSetGetScoreResponse;
import grpc.cache_client._SortedSetIncrementRequest;
import grpc.cache_client._SortedSetIncrementResponse;
import grpc.cache_client._SortedSetPutRequest;
import grpc.cache_client._SortedSetPutResponse;
import grpc.cache_client._SortedSetRemoveRequest;
import grpc.cache_client._SortedSetRemoveResponse;
import grpc.cache_client._Unbounded;
import io.grpc.Metadata;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.exceptions.UnknownException;
import momento.sdk.messages.*;
import momento.sdk.requests.CollectionTtl;

/** Client for interacting with Scs Data plane. */
final class ScsDataClient extends ScsClient {

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
      String cacheName, String setName, Iterable<String> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(setName);
      ensureValidValue(elements);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElements(cacheName, convert(setName), convertStringIterable(elements), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetAddElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetAddElementsResponse> setAddElementsByteArray(
      String cacheName, String setName, Iterable<byte[]> elements, CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(setName);
      ensureValidValue(elements);
      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }
      return sendSetAddElements(
          cacheName, convert(setName), convertByteArrayIterable(elements), ttl);
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
      String cacheName, String setName, Iterable<String> elements) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(elements);
      return sendSetRemoveElements(cacheName, convert(setName), convertStringIterable(elements));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSetRemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSetRemoveElementsResponse> setRemoveElementsByteArray(
      String cacheName, String setName, Iterable<byte[]> elements) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(elements);
      return sendSetRemoveElements(cacheName, convert(setName), convertByteArrayIterable(elements));
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
      checkSortedSetNameValid(sortedSetName);
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
      checkSortedSetNameValid(sortedSetName);
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
      checkSortedSetNameValid(sortedSetName);
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
      checkSortedSetNameValid(sortedSetName);
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

  CompletableFuture<CacheSortedSetPutElementsResponse> sortedSetPutElements(
      String cacheName,
      String sortedSetName,
      Iterable<ScoredElement> elements,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(elements);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendSortedSetPutElements(cacheName, convert(sortedSetName), elements, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetPutElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetFetchResponse> sortedSetFetchByRank(
      String cacheName,
      String sortedSetName,
      @Nullable Integer startRank,
      @Nullable Integer endRank,
      @Nullable SortOrder order) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      checkIndexRangeValid(startRank, endRank);

      return sendSortedSetFetchByRank(cacheName, convert(sortedSetName), startRank, endRank, order);
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
      @Nullable SortOrder order,
      @Nullable Integer offset,
      @Nullable Integer count) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      checkScoreRangeValid(minScore, maxScore);
      checkSortedSetOffsetValid(offset);
      checkSortedSetCountValid(count);

      return sendSortedSetFetchByScore(
          cacheName, convert(sortedSetName), minScore, maxScore, order, offset, count);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetGetRankResponse> sortedSetGetRank(
      String cacheName, String sortedSetName, String value, @Nullable SortOrder order) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetGetRank(cacheName, convert(sortedSetName), convert(value), order);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetGetRankResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetGetRankResponse> sortedSetGetRank(
      String cacheName, String sortedSetName, byte[] value, @Nullable SortOrder order) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetGetRank(cacheName, convert(sortedSetName), convert(value), order);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetGetRankResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetGetScoreResponse> sortedSetGetScore(
      String cacheName, String sortedSetName, String value) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetGetScore(cacheName, convert(sortedSetName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetGetScoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetGetScoreResponse> sortedSetGetScore(
      String cacheName, String sortedSetName, byte[] value) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetGetScore(cacheName, convert(sortedSetName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetGetScoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetGetScoresResponse> sortedSetGetScores(
      String cacheName, String sortedSetName, Iterable<String> values) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(values);

      return sendSortedSetGetScores(
          cacheName, convert(sortedSetName), convertStringIterable(values));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetGetScoresResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetGetScoresResponse> sortedSetGetScoresByteArray(
      String cacheName, String sortedSetName, Iterable<byte[]> values) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(values);

      return sendSortedSetGetScores(
          cacheName, convert(sortedSetName), convertByteArrayIterable(values));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetGetScoresResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetIncrementScoreResponse> sortedSetIncrementScore(
      String cacheName,
      String sortedSetName,
      String value,
      double amount,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendSortedSetIncrementScore(
          cacheName, convert(sortedSetName), convert(value), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetIncrementScoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetIncrementScoreResponse> sortedSetIncrementScore(
      String cacheName,
      String sortedSetName,
      byte[] value,
      double amount,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendSortedSetIncrementScore(
          cacheName, convert(sortedSetName), convert(value), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetIncrementScoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetRemoveElementResponse> sortedSetRemoveElement(
      String cacheName, String sortedSetName, String value) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetRemoveElement(cacheName, convert(sortedSetName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetRemoveElementResponse> sortedSetRemoveElement(
      String cacheName, String sortedSetName, byte[] value) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetRemoveElement(cacheName, convert(sortedSetName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetRemoveElementsResponse> sortedSetRemoveElements(
      String cacheName, String sortedSetName, Iterable<String> values) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(values);

      return sendSortedSetRemoveElements(
          cacheName, convert(sortedSetName), convertStringIterable(values));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetRemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheSortedSetRemoveElementsResponse> sortedSetRemoveElementsByteArray(
      String cacheName, String sortedSetName, Iterable<byte[]> values) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(values);

      return sendSortedSetRemoveElements(
          cacheName, convert(sortedSetName), convertByteArrayIterable(values));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheSortedSetRemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBack(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<String> values,
      @Nullable Integer truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(values);
      ensureValidTruncateToSize(truncateFrontToSize);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListConcatenateBack(
          cacheName, convert(listName), convertStringIterable(values), truncateFrontToSize, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateBackResponse> listConcatenateBackByteArray(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<byte[]> values,
      @Nullable Integer truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(values);
      ensureValidTruncateToSize(truncateFrontToSize);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListConcatenateBack(
          cacheName, convert(listName), convertByteArrayIterable(values), truncateFrontToSize, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFront(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<String> values,
      @Nullable Integer truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(values);
      ensureValidTruncateToSize(truncateBackToSize);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListConcatenateFront(
          cacheName, convert(listName), convertStringIterable(values), truncateBackToSize, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListConcatenateFrontResponse> listConcatenateFrontByteArray(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull Iterable<byte[]> values,
      @Nullable Integer truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(values);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListConcatenateFront(
          cacheName, convert(listName), convertByteArrayIterable(values), truncateBackToSize, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListConcatenateFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListFetchResponse> listFetch(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nullable Integer startIndex,
      @Nullable Integer endIndex) {
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

  CompletableFuture<CacheListLengthResponse> listLength(
      @Nonnull String cacheName, @Nonnull String listName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      return sendListLength(cacheName, convert(listName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListLengthResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPopBackResponse> listPopBack(
      @Nonnull String cacheName, @Nonnull String listName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      return sendListPopBack(cacheName, convert(listName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPopBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPopFrontResponse> listPopFront(
      @Nonnull String cacheName, @Nonnull String listName) {
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
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull String value,
      @Nullable Integer truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);
      ensureValidTruncateToSize(truncateFrontToSize);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListPushBack(
          cacheName, convert(listName), convert(value), truncateFrontToSize, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPushBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPushBackResponse> listPushBack(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull byte[] value,
      @Nullable Integer truncateFrontToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);
      ensureValidTruncateToSize(truncateFrontToSize);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListPushBack(
          cacheName, convert(listName), convert(value), truncateFrontToSize, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPushBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPushFrontResponse> listPushFront(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull String value,
      @Nullable Integer truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);
      ensureValidTruncateToSize(truncateBackToSize);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListPushFront(
          cacheName, convert(listName), convert(value), truncateBackToSize, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPushFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListPushFrontResponse> listPushFront(
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nonnull byte[] value,
      @Nullable Integer truncateBackToSize,
      @Nullable CollectionTtl ttl) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);
      ensureValidTruncateToSize(truncateBackToSize);

      if (ttl == null) {
        ttl = CollectionTtl.of(itemDefaultTtl);
      }

      return sendListPushFront(
          cacheName, convert(listName), convert(value), truncateBackToSize, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new CacheListPushFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<CacheListRemoveValueResponse> listRemoveValue(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull String value) {
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
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull byte[] value) {
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
      @Nonnull String cacheName,
      @Nonnull String listName,
      @Nullable Integer startIndex,
      @Nullable Integer endIndex) {
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

  private ByteString convert(String string) {
    if (string == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFromUtf8(string);
  }

  private ByteString convert(byte[] bytes) {
    if (bytes == null) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(bytes);
  }

  private List<ByteString> convertStringIterable(Iterable<String> strings) {
    return StreamSupport.stream(strings.spliterator(), false)
        .map(this::convert)
        .collect(Collectors.toList());
  }

  private List<ByteString> convertByteArrayIterable(Iterable<byte[]> byteArrays) {
    return StreamSupport.stream(byteArrays.spliterator(), false)
        .map(this::convert)
        .collect(Collectors.toList());
  }

  private List<ByteString> convertStringList(List<String> strings) {
    return strings.stream().map(this::convert).collect(Collectors.toList());
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

  private List<ScoredElement> convertStringScoreMap(Map<String, Double> elements) {
    return elements.entrySet().stream()
        .map(entry -> new ScoredElement(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  private List<ScoredElement> convertBytesScoreMap(Map<byte[], Double> elements) {
    return elements.entrySet().stream()
        .map(entry -> new ScoredElement(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
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

    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SetIfNotExistsResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .setIfNotExists(buildSetIfNotExistsRequest(key, value, ttl));

    final Function<_SetIfNotExistsResponse, CacheSetIfNotExistsResponse> success =
        rsp -> {
          if (rsp.getResultCase().equals(_SetIfNotExistsResponse.ResultCase.STORED)) {
            return new CacheSetIfNotExistsResponse.Stored(key, value);
          } else if (rsp.getResultCase().equals(_SetIfNotExistsResponse.ResultCase.NOT_STORED)) {
            return new CacheSetIfNotExistsResponse.NotStored();
          } else {
            return new CacheSetIfNotExistsResponse.Error(
                new UnknownException(
                    "Unrecognized set-if-not-exists result: " + rsp.getResultCase()));
          }
        };

    final Function<Throwable, CacheSetIfNotExistsResponse> failure =
        e ->
            new CacheSetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
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
      String cacheName, ByteString setName, Iterable<ByteString> elements, CollectionTtl ttl) {

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
      String cacheName, ByteString setName, Iterable<ByteString> elements) {

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
      ByteString sortedSetName,
      ByteString value,
      double score,
      CollectionTtl collectionTtl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SortedSetPutResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .sortedSetPut(
                buildSortedSetPutRequest(
                    sortedSetName,
                    Collections.singletonList(new ScoredElement(value, score)),
                    collectionTtl));

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
      ByteString sortedSetName,
      Iterable<ScoredElement> elements,
      CollectionTtl collectionTtl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SortedSetPutResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .sortedSetPut(buildSortedSetPutRequest(sortedSetName, elements, collectionTtl));

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
      ByteString sortedSetName,
      @Nullable Integer startRank,
      @Nullable Integer endRank,
      @Nullable SortOrder order) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SortedSetFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .sortedSetFetch(
                buildSortedSetFetchRequestByRank(sortedSetName, startRank, endRank, order));

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
      ByteString sortedSetName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable SortOrder order,
      @Nullable Integer offset,
      @Nullable Integer count) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SortedSetFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .sortedSetFetch(
                buildSortedSetFetchRequestByScore(
                    sortedSetName, minScore, maxScore, order, offset, count));

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

  private CompletableFuture<CacheSortedSetGetRankResponse> sendSortedSetGetRank(
      String cacheName, ByteString sortedSetName, ByteString value, @Nullable SortOrder order) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetGetRankResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetGetRank(buildSortedSetGetRank(sortedSetName, value, order));

    final Function<_SortedSetGetRankResponse, CacheSortedSetGetRankResponse> success =
        rsp -> {
          if (rsp.hasElementRank()) {
            return new CacheSortedSetGetRankResponse.Hit(rsp.getElementRank().getRank());
          } else {
            return new CacheSortedSetGetRankResponse.Miss();
          }
        };

    final Function<Throwable, CacheSortedSetGetRankResponse> failure =
        e ->
            new CacheSortedSetGetRankResponse.Error(
                CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CacheSortedSetGetScoreResponse> sendSortedSetGetScore(
      String cacheName, ByteString sortedSetName, ByteString value) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetGetScoreResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetGetScore(
                    buildSortedSetGetScores(sortedSetName, Collections.singletonList(value)));

    final Function<_SortedSetGetScoreResponse, CacheSortedSetGetScoreResponse> success =
        rsp -> {
          if (rsp.hasFound()) {
            final Optional<_SortedSetGetScoreResponse._SortedSetGetScoreResponsePart> partOpt =
                rsp.getFound().getElementsList().stream().findFirst();
            if (partOpt.isPresent()) {
              final _SortedSetGetScoreResponse._SortedSetGetScoreResponsePart part = partOpt.get();

              if (part.getResult().equals(ECacheResult.Hit)) {
                return new CacheSortedSetGetScoreResponse.Hit(value, part.getScore());
              } else if (part.getResult().equals(ECacheResult.Miss)) {
                return new CacheSortedSetGetScoreResponse.Miss(value);
              } else {
                return new CacheSortedSetGetScoreResponse.Error(
                    new UnknownException("Unrecognized result: " + part.getResult()));
              }
            } else {
              return new CacheSortedSetGetScoreResponse.Error(
                  new UnknownException("Response claimed results found but returned no results"));
            }

          } else {
            return new CacheSortedSetGetScoreResponse.Miss(value);
          }
        };

    final Function<Throwable, CacheSortedSetGetScoreResponse> failure =
        e ->
            new CacheSortedSetGetScoreResponse.Error(
                CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CacheSortedSetGetScoresResponse> sendSortedSetGetScores(
      String cacheName, ByteString sortedSetName, List<ByteString> values) {

    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetGetScoreResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetGetScore(buildSortedSetGetScores(sortedSetName, values));

    final Function<_SortedSetGetScoreResponse, CacheSortedSetGetScoresResponse> success =
        rsp -> {
          if (rsp.hasFound()) {
            final List<_SortedSetGetScoreResponse._SortedSetGetScoreResponsePart> scores =
                rsp.getFound().getElementsList();

            final List<CacheSortedSetGetScoreResponse> scoreResponses = new ArrayList<>();
            for (int i = 0; i < scores.size(); ++i) {
              final _SortedSetGetScoreResponse._SortedSetGetScoreResponsePart part = scores.get(i);
              if (part.getResult().equals(ECacheResult.Hit)) {
                scoreResponses.add(
                    new CacheSortedSetGetScoreResponse.Hit(values.get(i), part.getScore()));
              } else if (part.getResult().equals(ECacheResult.Miss)) {
                scoreResponses.add(new CacheSortedSetGetScoreResponse.Miss(values.get(i)));
              } else {
                scoreResponses.add(
                    new CacheSortedSetGetScoreResponse.Error(
                        new UnknownException("Unrecognized result: " + part.getResult())));
              }
            }
            return new CacheSortedSetGetScoresResponse.Hit(scoreResponses);
          } else {
            return new CacheSortedSetGetScoresResponse.Miss();
          }
        };

    final Function<Throwable, CacheSortedSetGetScoresResponse> failure =
        e ->
            new CacheSortedSetGetScoresResponse.Error(
                CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CacheSortedSetIncrementScoreResponse> sendSortedSetIncrementScore(
      String cacheName,
      ByteString sortedSetName,
      ByteString value,
      double amount,
      CollectionTtl ttl) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetIncrementResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetIncrement(buildSortedSetIncrement(sortedSetName, value, amount, ttl));

    final Function<_SortedSetIncrementResponse, CacheSortedSetIncrementScoreResponse> success =
        rsp -> new CacheSortedSetIncrementScoreResponse.Success(rsp.getScore());

    final Function<Throwable, CacheSortedSetIncrementScoreResponse> failure =
        e ->
            new CacheSortedSetIncrementScoreResponse.Error(
                CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CacheSortedSetRemoveElementResponse> sendSortedSetRemoveElement(
      String cacheName, ByteString sortedSetName, ByteString value) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetRemoveResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetRemove(buildSortedSetRemove(sortedSetName, Collections.singleton(value)));

    final Function<_SortedSetRemoveResponse, CacheSortedSetRemoveElementResponse> success =
        rsp -> new CacheSortedSetRemoveElementResponse.Success();

    final Function<Throwable, CacheSortedSetRemoveElementResponse> failure =
        e ->
            new CacheSortedSetRemoveElementResponse.Error(
                CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CacheSortedSetRemoveElementsResponse> sendSortedSetRemoveElements(
      String cacheName, ByteString sortedSetName, Iterable<ByteString> values) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetRemoveResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetRemove(buildSortedSetRemove(sortedSetName, values));

    final Function<_SortedSetRemoveResponse, CacheSortedSetRemoveElementsResponse> success =
        rsp -> new CacheSortedSetRemoveElementsResponse.Success();

    final Function<Throwable, CacheSortedSetRemoveElementsResponse> failure =
        e ->
            new CacheSortedSetRemoveElementsResponse.Error(
                CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<CacheListConcatenateBackResponse> sendListConcatenateBack(
      @Nonnull String cacheName,
      @Nonnull ByteString listName,
      @Nonnull List<ByteString> values,
      @Nullable Integer truncateFrontToSize,
      @Nonnull CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListConcatenateBackResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listConcatenateBack(
                buildListConcatenateBackRequest(listName, values, truncateFrontToSize, ttl));

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
      @Nonnull String cacheName,
      @Nonnull ByteString listName,
      @Nonnull List<ByteString> values,
      @Nullable Integer truncateBackToSize,
      @Nonnull CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListConcatenateFrontResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listConcatenateFront(
                buildListConcatenateFrontRequest(listName, values, truncateBackToSize, ttl));

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
      @Nonnull String cacheName,
      @Nonnull ByteString listName,
      @Nullable Integer startIndex,
      @Nullable Integer endIndex) {

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
      @Nonnull String cacheName, @Nonnull ByteString listName) {

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
      @Nonnull String cacheName, @Nonnull ByteString listName) {

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
      @Nonnull String cacheName,
      @Nonnull ByteString listName,
      @Nonnull ByteString value,
      @Nullable Integer truncateFrontToSize,
      @Nonnull CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListPushBackResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listPushBack(buildListPushBackRequest(listName, value, truncateFrontToSize, ttl));

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
      @Nonnull String cacheName, @Nonnull ByteString listName) {

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
      @Nonnull String cacheName,
      @Nonnull ByteString listName,
      @Nonnull ByteString value,
      @Nullable Integer truncateBackToSize,
      @Nonnull CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListPushFrontResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listPushFront(buildListPushFrontRequest(listName, value, truncateBackToSize, ttl));

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
      @Nonnull String cacheName, @Nonnull ByteString listName, @Nonnull ByteString value) {

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
      @Nonnull String cacheName,
      @Nonnull ByteString listName,
      @Nullable Integer startIndex,
      @Nullable Integer endIndex) {

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
              final Map<ByteString, ByteString> fieldsToValues =
                  rsp.getFound().getItemsList().stream()
                      .collect(
                          Collectors.toMap(
                              _DictionaryFieldValuePair::getField,
                              _DictionaryFieldValuePair::getValue));
              returnFuture.complete(new CacheDictionaryFetchResponse.Hit(fieldsToValues));
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
              final List<_DictionaryGetResponse._DictionaryGetResponsePart> elements =
                  rsp.getFound().getItemsList();

              final List<CacheDictionaryGetFieldResponse> responses = new ArrayList<>();
              for (int i = 0; i < elements.size(); ++i) {
                final _DictionaryGetResponse._DictionaryGetResponsePart part = elements.get(i);
                if (part.getResult().equals(ECacheResult.Hit)) {
                  responses.add(
                      new CacheDictionaryGetFieldResponse.Hit(fields.get(i), part.getCacheBody()));
                } else if (part.getResult().equals(ECacheResult.Miss)) {
                  responses.add(new CacheDictionaryGetFieldResponse.Miss(fields.get(i)));
                } else {
                  responses.add(
                      new CacheDictionaryGetFieldResponse.Error(
                          new UnknownException("Unrecognized result: " + part.getResult()),
                          fields.get(i)));
                }
              }
              returnFuture.complete(new CacheDictionaryGetFieldsResponse.Hit(responses));
            } else {
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
      ByteString setName, Iterable<ByteString> elements, CollectionTtl ttl) {
    return _SetUnionRequest.newBuilder()
        .setSetName(setName)
        .addAllElements(elements)
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private _SetDifferenceRequest buildSetDifferenceRequest(
      ByteString setName, Iterable<ByteString> elements) {
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
      ByteString sortedSetName, Iterable<ScoredElement> elements, CollectionTtl ttl) {
    return _SortedSetPutRequest.newBuilder()
        .setSetName(sortedSetName)
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .addAllElements(
            StreamSupport.stream(elements.spliterator(), false)
                .map(
                    e ->
                        _SortedSetElement.newBuilder()
                            .setValue(e.getValueByteString())
                            .setScore(e.getScore())
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  private _SortedSetFetchRequest buildSortedSetFetchRequestByRank(
      ByteString sortedSetName,
      @Nullable Integer startRank,
      @Nullable Integer endRank,
      @Nullable SortOrder order) {

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

    final _SortedSetFetchRequest.Builder requestBuilder =
        _SortedSetFetchRequest.newBuilder()
            .setSetName(sortedSetName)
            .setWithScores(true)
            .setByIndex(indexBuilder);

    if (order == SortOrder.DESCENDING) {
      requestBuilder.setOrder(_SortedSetFetchRequest.Order.DESCENDING);
    } else {
      requestBuilder.setOrder(_SortedSetFetchRequest.Order.ASCENDING);
    }

    return requestBuilder.build();
  }

  private _SortedSetFetchRequest buildSortedSetFetchRequestByScore(
      ByteString sortedSetName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable SortOrder order,
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

    final _SortedSetFetchRequest.Builder requestBuilder =
        _SortedSetFetchRequest.newBuilder()
            .setSetName(sortedSetName)
            .setWithScores(true)
            .setByScore(scoreBuilder);

    if (order == SortOrder.DESCENDING) {
      requestBuilder.setOrder(_SortedSetFetchRequest.Order.DESCENDING);
    } else {
      requestBuilder.setOrder(_SortedSetFetchRequest.Order.ASCENDING);
    }

    return requestBuilder.build();
  }

  private _SortedSetGetRankRequest buildSortedSetGetRank(
      ByteString sortedSetName, ByteString element, @Nullable SortOrder order) {
    final _SortedSetGetRankRequest.Builder requestBuilder =
        _SortedSetGetRankRequest.newBuilder().setSetName(sortedSetName).setValue(element);

    if (order == SortOrder.DESCENDING) {
      requestBuilder.setOrder(_SortedSetGetRankRequest.Order.DESCENDING);
    } else {
      requestBuilder.setOrder(_SortedSetGetRankRequest.Order.ASCENDING);
    }

    return requestBuilder.build();
  }

  private _SortedSetGetScoreRequest buildSortedSetGetScores(
      ByteString sortedSetName, Iterable<ByteString> values) {
    return _SortedSetGetScoreRequest.newBuilder()
        .setSetName(sortedSetName)
        .addAllValues(values)
        .build();
  }

  private _SortedSetIncrementRequest buildSortedSetIncrement(
      ByteString sortedSetName, ByteString element, double amount, CollectionTtl ttl) {
    return _SortedSetIncrementRequest.newBuilder()
        .setSetName(sortedSetName)
        .setValue(element)
        .setAmount(amount)
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private _SortedSetRemoveRequest buildSortedSetRemove(
      ByteString sortedSetName, Iterable<ByteString> values) {
    return _SortedSetRemoveRequest.newBuilder()
        .setSetName(sortedSetName)
        .setSome(_SortedSetRemoveRequest._Some.newBuilder().addAllValues(values).build())
        .build();
  }

  private _ListConcatenateBackRequest buildListConcatenateBackRequest(
      @Nonnull ByteString listName,
      @Nonnull List<ByteString> values,
      @Nullable Integer truncateFrontToSize,
      @Nonnull CollectionTtl ttl) {
    final _ListConcatenateBackRequest.Builder builder =
        _ListConcatenateBackRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .addAllValues(values);

    if (truncateFrontToSize != null) {
      builder.setTruncateFrontToSize(truncateFrontToSize);
    }

    return builder.build();
  }

  private _ListConcatenateFrontRequest buildListConcatenateFrontRequest(
      @Nonnull ByteString listName,
      @Nonnull List<ByteString> values,
      @Nullable Integer truncateBackToSize,
      @Nonnull CollectionTtl ttl) {
    final _ListConcatenateFrontRequest.Builder builder =
        _ListConcatenateFrontRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .addAllValues(values);

    if (truncateBackToSize != null) {
      builder.setTruncateBackToSize(truncateBackToSize);
    }

    return builder.build();
  }

  private _ListFetchRequest buildListFetchRequest(
      @Nonnull ByteString listName, @Nullable Integer startIndex, @Nullable Integer endIndex) {
    final _ListFetchRequest.Builder builder = _ListFetchRequest.newBuilder().setListName(listName);

    if (startIndex != null) {
      builder.setInclusiveStart(startIndex);
    } else {
      builder.setUnboundedStart(_Unbounded.newBuilder().build());
    }

    if (endIndex != null) {
      builder.setExclusiveEnd(endIndex);
    } else {
      builder.setUnboundedEnd(_Unbounded.newBuilder().build());
    }

    return builder.build();
  }

  private _ListLengthRequest buildListLengthRequest(@Nonnull ByteString listName) {
    return _ListLengthRequest.newBuilder().setListName(listName).build();
  }

  private _ListPopBackRequest buildListPopBackRequest(@Nonnull ByteString listName) {
    return _ListPopBackRequest.newBuilder().setListName(listName).build();
  }

  private _ListPopFrontRequest buildListPopFrontRequest(@Nonnull ByteString listName) {
    return _ListPopFrontRequest.newBuilder().setListName(listName).build();
  }

  private _ListPushBackRequest buildListPushBackRequest(
      @Nonnull ByteString listName,
      @Nonnull ByteString value,
      @Nullable Integer truncateFrontToSize,
      @Nonnull CollectionTtl ttl) {
    final _ListPushBackRequest.Builder builder =
        _ListPushBackRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .setValue(value);

    if (truncateFrontToSize != null) {
      builder.setTruncateFrontToSize(truncateFrontToSize);
    }

    return builder.build();
  }

  private _ListPushFrontRequest buildListPushFrontRequest(
      @Nonnull ByteString listName,
      @Nonnull ByteString value,
      @Nullable Integer truncateBackToSize,
      @Nonnull CollectionTtl ttl) {
    final _ListPushFrontRequest.Builder builder =
        _ListPushFrontRequest.newBuilder()
            .setListName(listName)
            .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
            .setRefreshTtl(ttl.refreshTtl())
            .setValue(value);

    if (truncateBackToSize != null) {
      builder.setTruncateBackToSize(truncateBackToSize);
    }

    return builder.build();
  }

  private _ListRemoveRequest buildListRemoveValueRequest(
      @Nonnull ByteString listName, @Nonnull ByteString value) {
    return _ListRemoveRequest.newBuilder()
        .setListName(listName)
        .setAllElementsWithValue(value)
        .build();
  }

  private _ListRetainRequest buildListRetainRequest(
      @Nonnull ByteString listName, @Nullable Integer startIndex, @Nullable Integer endIndex) {
    final _ListRetainRequest.Builder builder =
        _ListRetainRequest.newBuilder().setListName(listName);

    if (startIndex != null) {
      builder.setInclusiveStart(startIndex);
    } else {
      builder.setUnboundedStart(_Unbounded.newBuilder().build());
    }

    if (endIndex != null) {
      builder.setExclusiveEnd(endIndex);
    } else {
      builder.setUnboundedEnd(_Unbounded.newBuilder().build());
    }

    return builder.build();
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
    return _DictionaryFieldValuePair.newBuilder().setField(field).setValue(value).build();
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
      Map<ByteString, ByteString> fieldValuePairs) {
    return fieldValuePairs.entrySet().stream()
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
