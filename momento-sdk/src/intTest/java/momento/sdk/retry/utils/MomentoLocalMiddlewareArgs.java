package momento.sdk.retry.utils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import momento.sdk.exceptions.MomentoErrorCode;
import momento.sdk.exceptions.MomentoErrorCodeMetadataConverter;
import momento.sdk.retry.MomentoRpcMethod;
import momento.sdk.retry.MomentoRpcMethodMetadataConverter;
import org.slf4j.Logger;

public class MomentoLocalMiddlewareArgs {
  private final Logger logger;
  private final TestRetryMetricsCollector testMetricsCollector;
  private final String requestId;
  private final String returnError;
  private final List<String> errorRpcList;
  private final Integer errorCount;
  private final List<String> delayRpcList;
  private final Integer delayMillis;
  private final Integer delayCount;
  private final List<String> streamErrorRpcList;
  private final String streamError;
  private final Integer streamErrorMessageLimit;

  private MomentoLocalMiddlewareArgs(
      Logger logger,
      TestRetryMetricsCollector testMetricsCollector,
      String requestId,
      String returnError,
      List<String> errorRpcList,
      Integer errorCount,
      List<String> delayRpcList,
      Integer delayMillis,
      Integer delayCount,
      List<String> streamErrorRpcList,
      String streamError,
      Integer streamErrorMessageLimit) {
    this.logger = logger;
    this.testMetricsCollector = testMetricsCollector;
    this.requestId = requestId;
    this.returnError = returnError;
    this.errorRpcList = errorRpcList;
    this.errorCount = errorCount;
    this.delayRpcList = delayRpcList;
    this.delayMillis = delayMillis;
    this.delayCount = delayCount;
    this.streamErrorRpcList = streamErrorRpcList;
    this.streamError = streamError;
    this.streamErrorMessageLimit = streamErrorMessageLimit;
  }

  public static class Builder {
    private final Logger logger;
    private final String requestId;
    private TestRetryMetricsCollector testMetricsCollector;
    private String returnError = null;
    private List<String> errorRpcList = null;
    private Integer errorCount = null;
    private List<String> delayRpcList = null;
    private Integer delayMillis = null;
    private Integer delayCount = null;
    private List<String> streamErrorRpcList = null;
    private String streamError = null;
    private Integer streamErrorMessageLimit = null;

    public Builder(Logger logger, String requestId) {
      this.logger = logger;
      this.requestId = requestId;
    }

    public Builder testMetricsCollector(TestRetryMetricsCollector testMetricsCollector) {
      this.testMetricsCollector = testMetricsCollector;
      return this;
    }

    public Builder returnError(MomentoErrorCode returnError) {
      this.returnError = MomentoErrorCodeMetadataConverter.convert(returnError);
      return this;
    }

    public Builder errorRpcList(List<MomentoRpcMethod> errorRpcList) {
      this.errorRpcList = this.convertRPCs(errorRpcList);
      return this;
    }

    public Builder errorCount(Integer errorCount) {
      this.errorCount = errorCount;
      return this;
    }

    public Builder delayRpcList(List<MomentoRpcMethod> delayRpcList) {
      this.delayRpcList = this.convertRPCs(delayRpcList);
      return this;
    }

    public Builder delayMillis(Integer delayMillis) {
      this.delayMillis = delayMillis;
      return this;
    }

    public Builder delayCount(Integer delayCount) {
      this.delayCount = delayCount;
      return this;
    }

    public Builder streamErrorRpcList(List<MomentoRpcMethod> errorRpcList) {
      this.streamErrorRpcList = this.convertRPCs(errorRpcList);
      return this;
    }

    public Builder streamError(MomentoErrorCode streamError) {
      this.streamError = MomentoErrorCodeMetadataConverter.convert(streamError);
      return this;
    }

    public Builder streamErrorMessageLimit(Integer limit) {
      this.streamErrorMessageLimit = limit;
      return this;
    }

    public MomentoLocalMiddlewareArgs build() {
      return new MomentoLocalMiddlewareArgs(
          logger,
          testMetricsCollector,
          requestId,
          returnError,
          errorRpcList,
          errorCount,
          delayRpcList,
          delayMillis,
          delayCount,
          streamErrorRpcList,
          streamError,
          streamErrorMessageLimit);
    }

    private List<String> convertRPCs(List<MomentoRpcMethod> RPCs) {
      return RPCs.stream()
          .map(MomentoRpcMethodMetadataConverter::convert)
          .collect(Collectors.toList());
    }
  }

  public Logger getLogger() {
    return logger;
  }

  public String getRequestId() {
    return requestId;
  }

  public Optional<TestRetryMetricsCollector> getTestMetricsCollector() {
    return Optional.ofNullable(testMetricsCollector);
  }

  public Optional<String> getReturnError() {
    return Optional.ofNullable(returnError);
  }

  public Optional<List<String>> getErrorRpcList() {
    return Optional.ofNullable(errorRpcList);
  }

  public Optional<Integer> getErrorCount() {
    return Optional.ofNullable(errorCount);
  }

  public Optional<List<String>> getDelayRpcList() {
    return Optional.ofNullable(delayRpcList);
  }

  public Optional<Integer> getDelayMillis() {
    return Optional.ofNullable(delayMillis);
  }

  public Optional<Integer> getDelayCount() {
    return Optional.ofNullable(delayCount);
  }

  public Optional<List<String>> getStreamErrorRpcList() {
    return Optional.ofNullable(streamErrorRpcList);
  }

  public Optional<String> getStreamError() {
    return Optional.ofNullable(streamError);
  }

  public Optional<Integer> getStreamErrorMessageLimit() {
    return Optional.ofNullable(streamErrorMessageLimit);
  }
}
