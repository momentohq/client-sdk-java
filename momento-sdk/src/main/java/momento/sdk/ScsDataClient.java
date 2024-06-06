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
import static momento.sdk.ValidationUtils.ensureValidTtl;
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
import grpc.cache_client._ItemGetTtlRequest;
import grpc.cache_client._ItemGetTtlResponse;
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
import grpc.cache_client._UpdateTtlRequest;
import grpc.cache_client._UpdateTtlResponse;
import io.grpc.Metadata;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
import momento.sdk.requests.CollectionTtl;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.IncrementResponse;
import momento.sdk.responses.cache.SetIfNotExistsResponse;
import momento.sdk.responses.cache.SetResponse;
import momento.sdk.responses.cache.dictionary.DictionaryFetchResponse;
import momento.sdk.responses.cache.dictionary.DictionaryGetFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionaryGetFieldsResponse;
import momento.sdk.responses.cache.dictionary.DictionaryIncrementResponse;
import momento.sdk.responses.cache.dictionary.DictionaryRemoveFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionaryRemoveFieldsResponse;
import momento.sdk.responses.cache.dictionary.DictionarySetFieldResponse;
import momento.sdk.responses.cache.dictionary.DictionarySetFieldsResponse;
import momento.sdk.responses.cache.list.ListConcatenateBackResponse;
import momento.sdk.responses.cache.list.ListConcatenateFrontResponse;
import momento.sdk.responses.cache.list.ListFetchResponse;
import momento.sdk.responses.cache.list.ListLengthResponse;
import momento.sdk.responses.cache.list.ListPopBackResponse;
import momento.sdk.responses.cache.list.ListPopFrontResponse;
import momento.sdk.responses.cache.list.ListPushBackResponse;
import momento.sdk.responses.cache.list.ListPushFrontResponse;
import momento.sdk.responses.cache.list.ListRemoveValueResponse;
import momento.sdk.responses.cache.list.ListRetainResponse;
import momento.sdk.responses.cache.set.SetAddElementResponse;
import momento.sdk.responses.cache.set.SetAddElementsResponse;
import momento.sdk.responses.cache.set.SetFetchResponse;
import momento.sdk.responses.cache.set.SetRemoveElementResponse;
import momento.sdk.responses.cache.set.SetRemoveElementsResponse;
import momento.sdk.responses.cache.sortedset.ScoredElement;
import momento.sdk.responses.cache.sortedset.SortedSetFetchResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetRankResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetScoreResponse;
import momento.sdk.responses.cache.sortedset.SortedSetGetScoresResponse;
import momento.sdk.responses.cache.sortedset.SortedSetIncrementScoreResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementResponse;
import momento.sdk.responses.cache.sortedset.SortedSetPutElementsResponse;
import momento.sdk.responses.cache.sortedset.SortedSetRemoveElementResponse;
import momento.sdk.responses.cache.sortedset.SortedSetRemoveElementsResponse;
import momento.sdk.responses.cache.ttl.ItemGetTtlResponse;
import momento.sdk.responses.cache.ttl.UpdateTtlResponse;

/** Client for interacting with Scs Data plane. */
final class ScsDataClient extends ScsClientBase {

  private final Duration itemDefaultTtl;
  private final ScsDataGrpcStubsManager scsDataGrpcStubsManager;

  ScsDataClient(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull Configuration configuration,
      @Nonnull Duration defaultTtl) {
    this.itemDefaultTtl = defaultTtl;
    this.scsDataGrpcStubsManager = new ScsDataGrpcStubsManager(credentialProvider, configuration);
  }

  public void connect(final long eagerConnectionTimeout) {
    this.scsDataGrpcStubsManager.connect(eagerConnectionTimeout);
  }

  CompletableFuture<GetResponse> get(String cacheName, byte[] key) {
    try {
      ensureValidKey(key);
      return sendGet(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new GetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<GetResponse> get(String cacheName, String key) {
    try {
      ensureValidKey(key);
      return sendGet(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new GetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DeleteResponse> delete(String cacheName, byte[] key) {
    try {
      ensureValidKey(key);
      return sendDelete(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DeleteResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DeleteResponse> delete(String cacheName, String key) {
    try {
      ensureValidKey(key);
      return sendDelete(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DeleteResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetResponse> set(
      String cacheName, byte[] key, byte[] value, @Nullable Duration ttl) {
    try {
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      ensureValidCacheSet(key, value, ttl);
      return sendSet(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetResponse> set(
      String cacheName, String key, String value, @Nullable Duration ttl) {
    try {
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      ensureValidCacheSet(key, value, ttl);
      return sendSet(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<IncrementResponse> increment(
      String cacheName, String field, long amount, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendIncrement(cacheName, convert(field), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new IncrementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<IncrementResponse> increment(
      String cacheName, byte[] field, long amount, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendIncrement(cacheName, convert(field), amount, ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new IncrementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, String value, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
      String cacheName, String key, byte[] value, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, String value, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetIfNotExistsResponse> setIfNotExists(
      String cacheName, byte[] key, byte[] value, @Nullable Duration ttl) {
    try {
      checkCacheNameValid(cacheName);
      if (ttl == null) {
        ttl = itemDefaultTtl;
      }
      return sendSetIfNotExists(cacheName, convert(key), convert(value), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<UpdateTtlResponse> updateTtl(String cacheName, String key, Duration ttl) {
    try {
      ensureValidKey(key);
      ensureValidTtl(ttl);
      checkCacheNameValid(cacheName);
      return sendUpdateTtl(cacheName, convert(key), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new UpdateTtlResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<UpdateTtlResponse> updateTtl(String cacheName, byte[] key, Duration ttl) {
    try {
      ensureValidKey(key);
      ensureValidTtl(ttl);
      checkCacheNameValid(cacheName);
      return sendUpdateTtl(cacheName, convert(key), ttl);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new UpdateTtlResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ItemGetTtlResponse> itemGetTtl(String cacheName, String key) {
    try {
      ensureValidKey(key);
      checkCacheNameValid(cacheName);
      return sendItemGetTtl(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ItemGetTtlResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ItemGetTtlResponse> itemGetTtl(String cacheName, byte[] key) {
    try {
      ensureValidKey(key);
      checkCacheNameValid(cacheName);
      return sendItemGetTtl(cacheName, convert(key));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ItemGetTtlResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetAddElementResponse> setAddElement(
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
          new SetAddElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetAddElementResponse> setAddElement(
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
          new SetAddElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetAddElementsResponse> setAddElements(
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
          new SetAddElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetAddElementsResponse> setAddElementsByteArray(
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
          new SetAddElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetRemoveElementResponse> setRemoveElement(
      String cacheName, String setName, String element) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      return sendSetRemoveElement(cacheName, convert(setName), convert(element));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetRemoveElementResponse> setRemoveElement(
      String cacheName, String setName, byte[] element) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(element);
      return sendSetRemoveElement(cacheName, convert(setName), convert(element));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetRemoveElementsResponse> setRemoveElements(
      String cacheName, String setName, Iterable<String> elements) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(elements);
      return sendSetRemoveElements(cacheName, convert(setName), convertStringIterable(elements));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetRemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetRemoveElementsResponse> setRemoveElementsByteArray(
      String cacheName, String setName, Iterable<byte[]> elements) {
    try {
      checkCacheNameValid(cacheName);
      checkSetNameValid(setName);
      ensureValidValue(elements);
      return sendSetRemoveElements(cacheName, convert(setName), convertByteArrayIterable(elements));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetRemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SetFetchResponse> setFetch(String cacheName, String setName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(setName);
      return sendSetFetch(cacheName, convert(setName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SetFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetPutElementResponse> sortedSetPutElement(
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
          new SortedSetPutElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetPutElementResponse> sortedSetPutElement(
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
          new SortedSetPutElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetPutElementsResponse> sortedSetPutElements(
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
          new SortedSetPutElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetPutElementsResponse> sortedSetPutElementsByteArray(
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
          new SortedSetPutElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetPutElementsResponse> sortedSetPutElements(
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
          new SortedSetPutElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetFetchResponse> sortedSetFetchByRank(
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
          new SortedSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetFetchResponse> sortedSetFetchByScore(
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
          new SortedSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetGetRankResponse> sortedSetGetRank(
      String cacheName, String sortedSetName, String value, @Nullable SortOrder order) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetGetRank(cacheName, convert(sortedSetName), convert(value), order);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetGetRankResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetGetRankResponse> sortedSetGetRank(
      String cacheName, String sortedSetName, byte[] value, @Nullable SortOrder order) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetGetRank(cacheName, convert(sortedSetName), convert(value), order);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetGetRankResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetGetScoreResponse> sortedSetGetScore(
      String cacheName, String sortedSetName, String value) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetGetScore(cacheName, convert(sortedSetName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetGetScoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetGetScoreResponse> sortedSetGetScore(
      String cacheName, String sortedSetName, byte[] value) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetGetScore(cacheName, convert(sortedSetName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetGetScoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetGetScoresResponse> sortedSetGetScores(
      String cacheName, String sortedSetName, Iterable<String> values) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(values);

      return sendSortedSetGetScores(
          cacheName, convert(sortedSetName), convertStringIterable(values));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetGetScoresResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetGetScoresResponse> sortedSetGetScoresByteArray(
      String cacheName, String sortedSetName, Iterable<byte[]> values) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(values);

      return sendSortedSetGetScores(
          cacheName, convert(sortedSetName), convertByteArrayIterable(values));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetGetScoresResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetIncrementScoreResponse> sortedSetIncrementScore(
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
          new SortedSetIncrementScoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetIncrementScoreResponse> sortedSetIncrementScore(
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
          new SortedSetIncrementScoreResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetRemoveElementResponse> sortedSetRemoveElement(
      String cacheName, String sortedSetName, String value) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetRemoveElement(cacheName, convert(sortedSetName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetRemoveElementResponse> sortedSetRemoveElement(
      String cacheName, String sortedSetName, byte[] value) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(value);

      return sendSortedSetRemoveElement(cacheName, convert(sortedSetName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetRemoveElementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetRemoveElementsResponse> sortedSetRemoveElements(
      String cacheName, String sortedSetName, Iterable<String> values) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(values);

      return sendSortedSetRemoveElements(
          cacheName, convert(sortedSetName), convertStringIterable(values));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetRemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<SortedSetRemoveElementsResponse> sortedSetRemoveElementsByteArray(
      String cacheName, String sortedSetName, Iterable<byte[]> values) {
    try {
      checkCacheNameValid(cacheName);
      checkSortedSetNameValid(sortedSetName);
      ensureValidValue(values);

      return sendSortedSetRemoveElements(
          cacheName, convert(sortedSetName), convertByteArrayIterable(values));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new SortedSetRemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListConcatenateBackResponse> listConcatenateBack(
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
          new ListConcatenateBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListConcatenateBackResponse> listConcatenateBackByteArray(
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
          new ListConcatenateBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListConcatenateFrontResponse> listConcatenateFront(
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
          new ListConcatenateFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListConcatenateFrontResponse> listConcatenateFrontByteArray(
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
          new ListConcatenateFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListFetchResponse> listFetch(
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
          new ListFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListLengthResponse> listLength(
      @Nonnull String cacheName, @Nonnull String listName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      return sendListLength(cacheName, convert(listName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ListLengthResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListPopBackResponse> listPopBack(
      @Nonnull String cacheName, @Nonnull String listName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      return sendListPopBack(cacheName, convert(listName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ListPopBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListPopFrontResponse> listPopFront(
      @Nonnull String cacheName, @Nonnull String listName) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      return sendListPopFront(cacheName, convert(listName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ListPopFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListPushBackResponse> listPushBack(
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
          new ListPushBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListPushBackResponse> listPushBack(
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
          new ListPushBackResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListPushFrontResponse> listPushFront(
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
          new ListPushFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListPushFrontResponse> listPushFront(
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
          new ListPushFrontResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListRemoveValueResponse> listRemoveValue(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull String value) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);

      return sendListRemoveValue(cacheName, convert(listName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ListRemoveValueResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListRemoveValueResponse> listRemoveValue(
      @Nonnull String cacheName, @Nonnull String listName, @Nonnull byte[] value) {
    try {
      checkCacheNameValid(cacheName);
      checkListNameValid(listName);
      ensureValidValue(value);

      return sendListRemoveValue(cacheName, convert(listName), convert(value));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new ListRemoveValueResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<ListRetainResponse> listRetain(
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
          new ListRetainResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DictionaryFetchResponse> dictionaryFetch(
      @Nonnull String cacheName, @Nonnull String dictionaryName) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);

      return sendDictionaryFetch(cacheName, convert(dictionaryName));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DictionaryFetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DictionarySetFieldResponse> dictionarySetField(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull String field,
      @Nonnull String value,
      @Nullable CollectionTtl ttl) {
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
          new DictionarySetFieldResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DictionarySetFieldResponse> dictionarySetField(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull String field,
      @Nonnull byte[] value,
      @Nullable CollectionTtl ttl) {
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
          new DictionarySetFieldResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DictionarySetFieldsResponse> dictionarySetFields(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull Map<String, String> elements,
      @Nullable CollectionTtl ttl) {
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
          new DictionarySetFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DictionarySetFieldsResponse> dictionarySetFieldsStringBytes(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull Map<String, byte[]> elements,
      @Nullable CollectionTtl ttl) {
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
          new DictionarySetFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DictionaryGetFieldResponse> dictionaryGetField(
      @Nonnull String cacheName, @Nonnull String dictionaryName, @Nonnull String field) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);

      return sendDictionaryGetField(cacheName, convert(dictionaryName), convert(field));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DictionaryGetFieldResponse.Error(
              CacheServiceExceptionMapper.convert(e), convert(field)));
    }
  }

  CompletableFuture<DictionaryGetFieldsResponse> dictionaryGetFields(
      @Nonnull String cacheName, @Nonnull String dictionaryName, @Nonnull Iterable<String> fields) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(fields);
      for (String field : fields) {
        ensureValidKey(field);
      }

      return sendDictionaryGetFields(
          cacheName, convert(dictionaryName), convertStringIterable(fields));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DictionaryGetFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DictionaryIncrementResponse> dictionaryIncrement(
      @Nonnull String cacheName,
      @Nonnull String dictionaryName,
      @Nonnull String field,
      long amount,
      @Nullable CollectionTtl ttl) {
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
          new DictionaryIncrementResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DictionaryRemoveFieldResponse> dictionaryRemoveField(
      @Nonnull String cacheName, @Nonnull String dictionaryName, @Nonnull String field) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(field);

      return sendDictionaryRemoveField(cacheName, convert(dictionaryName), convert(field));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DictionaryRemoveFieldResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  CompletableFuture<DictionaryRemoveFieldsResponse> dictionaryRemoveFields(
      @Nonnull String cacheName, @Nonnull String dictionaryName, @Nonnull Iterable<String> fields) {
    try {
      checkCacheNameValid(cacheName);
      checkDictionaryNameValid(dictionaryName);
      ensureValidKey(fields);
      for (String field : fields) {
        ensureValidKey(field);
      }

      return sendDictionaryRemoveFields(
          cacheName, convert(dictionaryName), convertStringIterable(fields));
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DictionaryRemoveFieldsResponse.Error(CacheServiceExceptionMapper.convert(e)));
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

  private CompletableFuture<GetResponse> sendGet(String cacheName, ByteString key) {
    checkCacheNameValid(cacheName);

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_GetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata).get(buildGetRequest(key));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<GetResponse> returnFuture =
        new CompletableFuture<GetResponse>() {
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

            final GetResponse response;
            if (result == ECacheResult.Hit) {
              response = new GetResponse.Hit(rsp.getCacheBody());
            } else if (result == ECacheResult.Miss) {
              response = new GetResponse.Miss();
            } else {
              response =
                  new GetResponse.Error(
                      new InternalServerException("Unsupported cache Get result: " + result));
            }
            returnFuture.complete(response);
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new GetResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<DeleteResponse> sendDelete(String cacheName, ByteString key) {
    checkCacheNameValid(cacheName);
    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DeleteResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata).delete(buildDeleteRequest(key));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DeleteResponse> returnFuture =
        new CompletableFuture<DeleteResponse>() {
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
            returnFuture.complete(new DeleteResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DeleteResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SetResponse> sendSet(
      String cacheName, ByteString key, ByteString value, Duration ttl) {
    checkCacheNameValid(cacheName);

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .set(buildSetRequest(key, value, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<SetResponse> returnFuture =
        new CompletableFuture<SetResponse>() {
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
            returnFuture.complete(new SetResponse.Success(value));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SetResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<IncrementResponse> sendIncrement(
      String cacheName, ByteString field, long amount, Duration ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_IncrementResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .increment(buildIncrementRequest(field, amount, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<IncrementResponse> returnFuture =
        new CompletableFuture<IncrementResponse>() {
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
            returnFuture.complete(new IncrementResponse.Success((int) rsp.getValue()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new IncrementResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<SetIfNotExistsResponse> sendSetIfNotExists(
      String cacheName, ByteString key, ByteString value, Duration ttl) {

    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SetIfNotExistsResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .setIfNotExists(buildSetIfNotExistsRequest(key, value, ttl));

    final Function<_SetIfNotExistsResponse, SetIfNotExistsResponse> success =
        rsp -> {
          if (rsp.getResultCase().equals(_SetIfNotExistsResponse.ResultCase.STORED)) {
            return new SetIfNotExistsResponse.Stored(key, value);
          } else if (rsp.getResultCase().equals(_SetIfNotExistsResponse.ResultCase.NOT_STORED)) {
            return new SetIfNotExistsResponse.NotStored();
          } else {
            return new SetIfNotExistsResponse.Error(
                new UnknownException(
                    "Unrecognized set-if-not-exists result: " + rsp.getResultCase()));
          }
        };

    final Function<Throwable, SetIfNotExistsResponse> failure =
        e -> new SetIfNotExistsResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<UpdateTtlResponse> sendUpdateTtl(
      String cacheName, ByteString key, Duration ttl) {

    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_UpdateTtlResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .updateTtl(buildUpdateTtlRequest(key, ttl));

    final Function<_UpdateTtlResponse, UpdateTtlResponse> success =
        rsp -> {
          if (rsp.getResultCase().equals(_UpdateTtlResponse.ResultCase.SET)) {
            return new UpdateTtlResponse.Set();
          } else if (rsp.getResultCase().equals(_UpdateTtlResponse.ResultCase.MISSING)) {
            return new UpdateTtlResponse.Miss();
          } else {
            return new UpdateTtlResponse.Error(
                new UnknownException("Unrecognized update-ttl result: " + rsp.getResultCase()));
          }
        };

    final Function<Throwable, UpdateTtlResponse> failure =
        e -> new UpdateTtlResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<ItemGetTtlResponse> sendItemGetTtl(String cacheName, ByteString key) {

    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_ItemGetTtlResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .itemGetTtl(buildItemGetTtlRequest(key));

    final Function<_ItemGetTtlResponse, ItemGetTtlResponse> success =
        rsp -> {
          if (rsp.getResultCase().equals(_ItemGetTtlResponse.ResultCase.FOUND)) {
            return new ItemGetTtlResponse.Hit(
                Duration.of(rsp.getFound().getRemainingTtlMillis(), ChronoUnit.MILLIS));
          } else if (rsp.getResultCase().equals(_ItemGetTtlResponse.ResultCase.MISSING)) {
            return new ItemGetTtlResponse.Miss();
          } else {
            return new ItemGetTtlResponse.Error(
                new UnknownException("Unrecognized item-get-ttl result: " + rsp.getResultCase()));
          }
        };

    final Function<Throwable, ItemGetTtlResponse> failure =
        e -> new ItemGetTtlResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<SetAddElementResponse> sendSetAddElement(
      String cacheName, ByteString setName, ByteString element, CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetUnionResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setUnion(buildSetUnionRequest(setName, Collections.singleton(element), ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<SetAddElementResponse> returnFuture =
        new CompletableFuture<SetAddElementResponse>() {
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
            returnFuture.complete(new SetAddElementResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SetAddElementResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SetAddElementsResponse> sendSetAddElements(
      String cacheName, ByteString setName, Iterable<ByteString> elements, CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetUnionResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setUnion(buildSetUnionRequest(setName, elements, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<SetAddElementsResponse> returnFuture =
        new CompletableFuture<SetAddElementsResponse>() {
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
            returnFuture.complete(new SetAddElementsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SetAddElementsResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SetRemoveElementResponse> sendSetRemoveElement(
      String cacheName, ByteString setName, ByteString element) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetDifferenceResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setDifference(buildSetDifferenceRequest(setName, Collections.singleton(element)));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<SetRemoveElementResponse> returnFuture =
        new CompletableFuture<SetRemoveElementResponse>() {
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
            returnFuture.complete(new SetRemoveElementResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SetRemoveElementResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SetRemoveElementsResponse> sendSetRemoveElements(
      String cacheName, ByteString setName, Iterable<ByteString> elements) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetDifferenceResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setDifference(buildSetDifferenceRequest(setName, elements));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<SetRemoveElementsResponse> returnFuture =
        new CompletableFuture<SetRemoveElementsResponse>() {
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
            returnFuture.complete(new SetRemoveElementsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SetRemoveElementsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SetFetchResponse> sendSetFetch(String cacheName, ByteString setName) {
    checkCacheNameValid(cacheName);

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_SetFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .setFetch(buildSetFetchRequest(setName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<SetFetchResponse> returnFuture =
        new CompletableFuture<SetFetchResponse>() {
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
              returnFuture.complete(new SetFetchResponse.Hit(rsp.getFound().getElementsList()));
            } else {
              returnFuture.complete(new SetFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SetFetchResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SortedSetPutElementResponse> sendSortedSetPutElement(
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
    final CompletableFuture<SortedSetPutElementResponse> returnFuture =
        new CompletableFuture<SortedSetPutElementResponse>() {
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
            returnFuture.complete(new SortedSetPutElementResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SortedSetPutElementResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SortedSetPutElementsResponse> sendSortedSetPutElements(
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
    final CompletableFuture<SortedSetPutElementsResponse> returnFuture =
        new CompletableFuture<SortedSetPutElementsResponse>() {
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
            returnFuture.complete(new SortedSetPutElementsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SortedSetPutElementsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SortedSetFetchResponse> sendSortedSetFetchByRank(
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
    final CompletableFuture<SortedSetFetchResponse> returnFuture =
        new CompletableFuture<SortedSetFetchResponse>() {
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
                  new SortedSetFetchResponse.Hit(
                      rsp.getFound().getValuesWithScores().getElementsList()));
            } else {
              returnFuture.complete(new SortedSetFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SortedSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SortedSetFetchResponse> sendSortedSetFetchByScore(
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
    final CompletableFuture<SortedSetFetchResponse> returnFuture =
        new CompletableFuture<SortedSetFetchResponse>() {
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
                  new SortedSetFetchResponse.Hit(
                      rsp.getFound().getValuesWithScores().getElementsList()));
            } else {
              returnFuture.complete(new SortedSetFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new SortedSetFetchResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<SortedSetGetRankResponse> sendSortedSetGetRank(
      String cacheName, ByteString sortedSetName, ByteString value, @Nullable SortOrder order) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetGetRankResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetGetRank(buildSortedSetGetRank(sortedSetName, value, order));

    final Function<_SortedSetGetRankResponse, SortedSetGetRankResponse> success =
        rsp -> {
          if (rsp.hasElementRank()) {
            return new SortedSetGetRankResponse.Hit(rsp.getElementRank().getRank());
          } else {
            return new SortedSetGetRankResponse.Miss();
          }
        };

    final Function<Throwable, SortedSetGetRankResponse> failure =
        e -> new SortedSetGetRankResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<SortedSetGetScoreResponse> sendSortedSetGetScore(
      String cacheName, ByteString sortedSetName, ByteString value) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetGetScoreResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetGetScore(
                    buildSortedSetGetScores(sortedSetName, Collections.singletonList(value)));

    final Function<_SortedSetGetScoreResponse, SortedSetGetScoreResponse> success =
        rsp -> {
          if (rsp.hasFound()) {
            final Optional<_SortedSetGetScoreResponse._SortedSetGetScoreResponsePart> partOpt =
                rsp.getFound().getElementsList().stream().findFirst();
            if (partOpt.isPresent()) {
              final _SortedSetGetScoreResponse._SortedSetGetScoreResponsePart part = partOpt.get();

              if (part.getResult().equals(ECacheResult.Hit)) {
                return new SortedSetGetScoreResponse.Hit(value, part.getScore());
              } else if (part.getResult().equals(ECacheResult.Miss)) {
                return new SortedSetGetScoreResponse.Miss(value);
              } else {
                return new SortedSetGetScoreResponse.Error(
                    new UnknownException("Unrecognized result: " + part.getResult()));
              }
            } else {
              return new SortedSetGetScoreResponse.Error(
                  new UnknownException("Response claimed results found but returned no results"));
            }

          } else {
            return new SortedSetGetScoreResponse.Miss(value);
          }
        };

    final Function<Throwable, SortedSetGetScoreResponse> failure =
        e -> new SortedSetGetScoreResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<SortedSetGetScoresResponse> sendSortedSetGetScores(
      String cacheName, ByteString sortedSetName, List<ByteString> values) {

    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetGetScoreResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetGetScore(buildSortedSetGetScores(sortedSetName, values));

    final Function<_SortedSetGetScoreResponse, SortedSetGetScoresResponse> success =
        rsp -> {
          if (rsp.hasFound()) {
            final List<_SortedSetGetScoreResponse._SortedSetGetScoreResponsePart> scores =
                rsp.getFound().getElementsList();

            final List<SortedSetGetScoreResponse> scoreResponses = new ArrayList<>();
            for (int i = 0; i < scores.size(); ++i) {
              final _SortedSetGetScoreResponse._SortedSetGetScoreResponsePart part = scores.get(i);
              if (part.getResult().equals(ECacheResult.Hit)) {
                scoreResponses.add(
                    new SortedSetGetScoreResponse.Hit(values.get(i), part.getScore()));
              } else if (part.getResult().equals(ECacheResult.Miss)) {
                scoreResponses.add(new SortedSetGetScoreResponse.Miss(values.get(i)));
              } else {
                scoreResponses.add(
                    new SortedSetGetScoreResponse.Error(
                        new UnknownException("Unrecognized result: " + part.getResult())));
              }
            }
            return new SortedSetGetScoresResponse.Hit(scoreResponses);
          } else {
            return new SortedSetGetScoresResponse.Miss();
          }
        };

    final Function<Throwable, SortedSetGetScoresResponse> failure =
        e -> new SortedSetGetScoresResponse.Error(CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<SortedSetIncrementScoreResponse> sendSortedSetIncrementScore(
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

    final Function<_SortedSetIncrementResponse, SortedSetIncrementScoreResponse> success =
        rsp -> new SortedSetIncrementScoreResponse.Success(rsp.getScore());

    final Function<Throwable, SortedSetIncrementScoreResponse> failure =
        e ->
            new SortedSetIncrementScoreResponse.Error(
                CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<SortedSetRemoveElementResponse> sendSortedSetRemoveElement(
      String cacheName, ByteString sortedSetName, ByteString value) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetRemoveResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetRemove(buildSortedSetRemove(sortedSetName, Collections.singleton(value)));

    final Function<_SortedSetRemoveResponse, SortedSetRemoveElementResponse> success =
        rsp -> new SortedSetRemoveElementResponse.Success();

    final Function<Throwable, SortedSetRemoveElementResponse> failure =
        e ->
            new SortedSetRemoveElementResponse.Error(
                CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<SortedSetRemoveElementsResponse> sendSortedSetRemoveElements(
      String cacheName, ByteString sortedSetName, Iterable<ByteString> values) {
    final Metadata metadata = metadataWithCache(cacheName);

    final Supplier<ListenableFuture<_SortedSetRemoveResponse>> stubSupplier =
        () ->
            attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
                .sortedSetRemove(buildSortedSetRemove(sortedSetName, values));

    final Function<_SortedSetRemoveResponse, SortedSetRemoveElementsResponse> success =
        rsp -> new SortedSetRemoveElementsResponse.Success();

    final Function<Throwable, SortedSetRemoveElementsResponse> failure =
        e ->
            new SortedSetRemoveElementsResponse.Error(
                CacheServiceExceptionMapper.convert(e, metadata));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<ListConcatenateBackResponse> sendListConcatenateBack(
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
    final CompletableFuture<ListConcatenateBackResponse> returnFuture =
        new CompletableFuture<ListConcatenateBackResponse>() {
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
            returnFuture.complete(new ListConcatenateBackResponse.Success(rsp.getListLength()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListConcatenateBackResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<ListConcatenateFrontResponse> sendListConcatenateFront(
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
    final CompletableFuture<ListConcatenateFrontResponse> returnFuture =
        new CompletableFuture<ListConcatenateFrontResponse>() {
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
            returnFuture.complete(new ListConcatenateFrontResponse.Success(rsp.getListLength()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListConcatenateFrontResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<ListFetchResponse> sendListFetch(
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
    final CompletableFuture<ListFetchResponse> returnFuture =
        new CompletableFuture<ListFetchResponse>() {
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
              returnFuture.complete(new ListFetchResponse.Hit(rsp.getFound().getValuesList()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new ListFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListFetchResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<ListLengthResponse> sendListLength(
      @Nonnull String cacheName, @Nonnull ByteString listName) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListLengthResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listLength(buildListLengthRequest(listName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<ListLengthResponse> returnFuture =
        new CompletableFuture<ListLengthResponse>() {
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
              returnFuture.complete(new ListLengthResponse.Hit(rsp.getFound().getLength()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new ListLengthResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListLengthResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<ListPopBackResponse> sendListPopBack(
      @Nonnull String cacheName, @Nonnull ByteString listName) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListPopBackResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listPopBack(buildListPopBackRequest(listName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<ListPopBackResponse> returnFuture =
        new CompletableFuture<ListPopBackResponse>() {
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
              returnFuture.complete(new ListPopBackResponse.Hit(rsp.getFound().getBack()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new ListPopBackResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListPopBackResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<ListPushBackResponse> sendListPushBack(
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
    final CompletableFuture<ListPushBackResponse> returnFuture =
        new CompletableFuture<ListPushBackResponse>() {
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
            returnFuture.complete(new ListPushBackResponse.Success(rsp.getListLength()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListPushBackResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<ListPopFrontResponse> sendListPopFront(
      @Nonnull String cacheName, @Nonnull ByteString listName) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListPopFrontResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listPopFront(buildListPopFrontRequest(listName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<ListPopFrontResponse> returnFuture =
        new CompletableFuture<ListPopFrontResponse>() {
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
              returnFuture.complete(new ListPopFrontResponse.Hit(rsp.getFound().getFront()));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new ListPopFrontResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListPopFrontResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        // Execute on same thread that called execute on CompletionStage
        MoreExecutors.directExecutor());

    return returnFuture;
  }

  private CompletableFuture<ListPushFrontResponse> sendListPushFront(
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
    final CompletableFuture<ListPushFrontResponse> returnFuture =
        new CompletableFuture<ListPushFrontResponse>() {
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
            returnFuture.complete(new ListPushFrontResponse.Success(rsp.getListLength()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListPushFrontResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<ListRemoveValueResponse> sendListRemoveValue(
      @Nonnull String cacheName, @Nonnull ByteString listName, @Nonnull ByteString value) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_ListRemoveResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .listRemove(buildListRemoveValueRequest(listName, value));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<ListRemoveValueResponse> returnFuture =
        new CompletableFuture<ListRemoveValueResponse>() {
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
            returnFuture.complete(new ListRemoveValueResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListRemoveValueResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<ListRetainResponse> sendListRetain(
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
    final CompletableFuture<ListRetainResponse> returnFuture =
        new CompletableFuture<ListRetainResponse>() {
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
            returnFuture.complete(new ListRetainResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new ListRetainResponse.Error(CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<DictionaryFetchResponse> sendDictionaryFetch(
      @Nonnull String cacheName, @Nonnull ByteString dictionaryName) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryFetchResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryFetch(buildDictionaryFetchRequest(dictionaryName));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DictionaryFetchResponse> returnFuture =
        new CompletableFuture<DictionaryFetchResponse>() {
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
              returnFuture.complete(new DictionaryFetchResponse.Hit(fieldsToValues));
            } else if (rsp.hasMissing()) {
              returnFuture.complete(new DictionaryFetchResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DictionaryFetchResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<DictionarySetFieldResponse> sendDictionarySetField(
      @Nonnull String cacheName,
      @Nonnull ByteString dictionaryName,
      @Nonnull ByteString field,
      @Nonnull ByteString value,
      @Nonnull CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionarySetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionarySet(buildDictionarySetFieldRequest(dictionaryName, field, value, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DictionarySetFieldResponse> returnFuture =
        new CompletableFuture<DictionarySetFieldResponse>() {
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
            returnFuture.complete(new DictionarySetFieldResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DictionarySetFieldResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<DictionarySetFieldsResponse> sendDictionarySetFields(
      @Nonnull String cacheName,
      @Nonnull ByteString dictionaryName,
      @Nonnull Map<ByteString, ByteString> elements,
      @Nonnull CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionarySetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionarySet(buildDictionarySetFieldsRequest(dictionaryName, elements, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DictionarySetFieldsResponse> returnFuture =
        new CompletableFuture<DictionarySetFieldsResponse>() {
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
            returnFuture.complete(new DictionarySetFieldsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DictionarySetFieldsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<DictionaryGetFieldResponse> sendDictionaryGetField(
      @Nonnull String cacheName, @Nonnull ByteString dictionaryName, @Nonnull ByteString field) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryGetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryGet(buildDictionaryGetFieldRequest(dictionaryName, field));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DictionaryGetFieldResponse> returnFuture =
        new CompletableFuture<DictionaryGetFieldResponse>() {
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
              returnFuture.complete(new DictionaryGetFieldResponse.Miss(field));
            } else if (rsp.hasFound()) {
              if (rsp.getFound().getItemsList().size() == 0) {
                returnFuture.complete(
                    new DictionaryGetFieldResponse.Error(
                        CacheServiceExceptionMapper.convert(
                            new Exception(
                                "_DictionaryGetResponseResponse contained no data but was found"),
                            metadata),
                        field));
              } else if (rsp.getFound().getItemsList().get(0).getResult() == ECacheResult.Miss) {
                returnFuture.complete(new DictionaryGetFieldResponse.Miss(field));
              } else {
                returnFuture.complete(
                    new DictionaryGetFieldResponse.Hit(
                        field, rsp.getFound().getItemsList().get(0).getCacheBody()));
              }
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DictionaryGetFieldResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata), field));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<DictionaryGetFieldsResponse> sendDictionaryGetFields(
      @Nonnull String cacheName,
      @Nonnull ByteString dictionaryName,
      @Nonnull List<ByteString> fields) {

    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryGetResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryGet(buildDictionaryGetFieldsRequest(dictionaryName, fields));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DictionaryGetFieldsResponse> returnFuture =
        new CompletableFuture<DictionaryGetFieldsResponse>() {
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

              final List<DictionaryGetFieldResponse> responses = new ArrayList<>();
              for (int i = 0; i < elements.size(); ++i) {
                final _DictionaryGetResponse._DictionaryGetResponsePart part = elements.get(i);
                if (part.getResult().equals(ECacheResult.Hit)) {
                  responses.add(
                      new DictionaryGetFieldResponse.Hit(fields.get(i), part.getCacheBody()));
                } else if (part.getResult().equals(ECacheResult.Miss)) {
                  responses.add(new DictionaryGetFieldResponse.Miss(fields.get(i)));
                } else {
                  responses.add(
                      new DictionaryGetFieldResponse.Error(
                          new UnknownException("Unrecognized result: " + part.getResult()),
                          fields.get(i)));
                }
              }
              returnFuture.complete(new DictionaryGetFieldsResponse.Hit(responses));
            } else {
              returnFuture.complete(new DictionaryGetFieldsResponse.Miss());
            }
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DictionaryGetFieldsResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<DictionaryIncrementResponse> sendDictionaryIncrement(
      @Nonnull String cacheName,
      @Nonnull ByteString dictionaryName,
      @Nonnull ByteString field,
      long amount,
      @Nonnull CollectionTtl ttl) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryIncrementResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryIncrement(
                buildDictionaryIncrementRequest(dictionaryName, field, amount, ttl));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DictionaryIncrementResponse> returnFuture =
        new CompletableFuture<DictionaryIncrementResponse>() {
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
            returnFuture.complete(new DictionaryIncrementResponse.Success((int) rsp.getValue()));
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DictionaryIncrementResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<DictionaryRemoveFieldResponse> sendDictionaryRemoveField(
      @Nonnull String cacheName, @Nonnull ByteString dictionaryName, @Nonnull ByteString field) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryDeleteResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryDelete(buildDictionaryRemoveFieldRequest(dictionaryName, field));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DictionaryRemoveFieldResponse> returnFuture =
        new CompletableFuture<DictionaryRemoveFieldResponse>() {
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
            returnFuture.complete(new DictionaryRemoveFieldResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DictionaryRemoveFieldResponse.Error(
                    CacheServiceExceptionMapper.convert(e, metadata)));
          }
        },
        MoreExecutors
            .directExecutor()); // Execute on same thread that called execute on CompletionStage
    // returned

    return returnFuture;
  }

  private CompletableFuture<DictionaryRemoveFieldsResponse> sendDictionaryRemoveFields(
      @Nonnull String cacheName,
      @Nonnull ByteString dictionaryName,
      @Nonnull List<ByteString> fields) {

    // Submit request to non-blocking stub
    final Metadata metadata = metadataWithCache(cacheName);
    final ListenableFuture<_DictionaryDeleteResponse> rspFuture =
        attachMetadata(scsDataGrpcStubsManager.getStub(), metadata)
            .dictionaryDelete(buildDictionaryRemoveFieldsRequest(dictionaryName, fields));

    // Build a CompletableFuture to return to caller
    final CompletableFuture<DictionaryRemoveFieldsResponse> returnFuture =
        new CompletableFuture<DictionaryRemoveFieldsResponse>() {
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
            returnFuture.complete(new DictionaryRemoveFieldsResponse.Success());
          }

          @Override
          public void onFailure(@Nonnull Throwable e) {
            returnFuture.complete(
                new DictionaryRemoveFieldsResponse.Error(
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

  private _UpdateTtlRequest buildUpdateTtlRequest(@Nonnull ByteString key, @Nonnull Duration ttl) {
    return _UpdateTtlRequest.newBuilder()
        .setCacheKey(key)
        .setOverwriteToMilliseconds(ttl.toMillis())
        .build();
  }

  private _ItemGetTtlRequest buildItemGetTtlRequest(@Nonnull ByteString key) {
    return _ItemGetTtlRequest.newBuilder().setCacheKey(key).build();
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
      indexBuilder.setUnboundedStart(indexBuilder.getUnboundedStart());
    }
    if (endRank != null) {
      indexBuilder.setExclusiveEndIndex(endRank);
    } else {
      indexBuilder.setUnboundedEnd(indexBuilder.getUnboundedEnd());
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
      scoreBuilder.setUnboundedMin(scoreBuilder.getUnboundedMin());
    }
    if (maxScore != null) {
      scoreBuilder.setMaxScore(
          _SortedSetFetchRequest._ByScore._Score.newBuilder().setScore(maxScore));
    } else {
      scoreBuilder.setUnboundedMax(scoreBuilder.getUnboundedMax());
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
      builder.setUnboundedStart(builder.getUnboundedStart());
    }

    if (endIndex != null) {
      builder.setExclusiveEnd(endIndex);
    } else {
      builder.setUnboundedEnd(builder.getUnboundedEnd());
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
      builder.setUnboundedStart(builder.getUnboundedStart());
    }

    if (endIndex != null) {
      builder.setExclusiveEnd(endIndex);
    } else {
      builder.setUnboundedEnd(builder.getUnboundedEnd());
    }

    return builder.build();
  }

  private _DictionaryFetchRequest buildDictionaryFetchRequest(@Nonnull ByteString dictionaryName) {
    return _DictionaryFetchRequest.newBuilder().setDictionaryName(dictionaryName).build();
  }

  private _DictionarySetRequest buildDictionarySetFieldRequest(
      @Nonnull ByteString dictionaryName,
      @Nonnull ByteString field,
      @Nonnull ByteString value,
      @Nonnull CollectionTtl ttl) {
    return _DictionarySetRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .addItems(toSingletonFieldValuePair(field, value))
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private _DictionaryFieldValuePair toSingletonFieldValuePair(
      @Nonnull ByteString field, @Nonnull ByteString value) {
    return _DictionaryFieldValuePair.newBuilder().setField(field).setValue(value).build();
  }

  private _DictionarySetRequest buildDictionarySetFieldsRequest(
      @Nonnull ByteString dictionaryName,
      @Nonnull Map<ByteString, ByteString> elements,
      @Nonnull CollectionTtl ttl) {
    return _DictionarySetRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .addAllItems(toDictionaryFieldValuePairs(elements))
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private List<_DictionaryFieldValuePair> toDictionaryFieldValuePairs(
      @Nonnull Map<ByteString, ByteString> fieldValuePairs) {
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
      @Nonnull ByteString dictionaryName, @Nonnull ByteString field) {
    return _DictionaryGetRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .addFields(field)
        .build();
  }

  private _DictionaryGetRequest buildDictionaryGetFieldsRequest(
      @Nonnull ByteString dictionaryName, @Nonnull List<ByteString> fields) {
    return _DictionaryGetRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .addAllFields(fields)
        .build();
  }

  private _DictionaryIncrementRequest buildDictionaryIncrementRequest(
      @Nonnull ByteString dictionaryName,
      @Nonnull ByteString field,
      long amount,
      @Nonnull CollectionTtl ttl) {
    return _DictionaryIncrementRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .setField(field)
        .setAmount(amount)
        .setTtlMilliseconds(ttl.toMilliseconds().orElse(itemDefaultTtl.toMillis()))
        .setRefreshTtl(ttl.refreshTtl())
        .build();
  }

  private _DictionaryDeleteRequest buildDictionaryRemoveFieldRequest(
      @Nonnull ByteString dictionaryName, @Nonnull ByteString field) {
    return _DictionaryDeleteRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .setSome(addSomeFieldsToRemove(field))
        .build();
  }

  private _DictionaryDeleteRequest.Some addSomeFieldsToRemove(@Nonnull ByteString field) {
    return _DictionaryDeleteRequest.Some.newBuilder().addFields(field).build();
  }

  private _DictionaryDeleteRequest buildDictionaryRemoveFieldsRequest(
      @Nonnull ByteString dictionaryName, @Nonnull List<ByteString> fields) {
    return _DictionaryDeleteRequest.newBuilder()
        .setDictionaryName(dictionaryName)
        .setSome(addSomeFieldsToRemove(fields))
        .build();
  }

  private _DictionaryDeleteRequest.Some addSomeFieldsToRemove(@Nonnull List<ByteString> fields) {
    return _DictionaryDeleteRequest.Some.newBuilder().addAllFields(fields).build();
  }

  @Override
  public void close() {
    scsDataGrpcStubsManager.close();
  }
}
