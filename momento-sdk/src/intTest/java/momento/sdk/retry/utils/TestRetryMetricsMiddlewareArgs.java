package momento.sdk.retry.utils;

import java.util.List;
import org.slf4j.Logger;

public class TestRetryMetricsMiddlewareArgs {
  private final Logger logger;
  private final TestRetryMetricsCollector testMetricsCollector;
  private final String requestId;
  private final String returnError;
  private final List<String> errorRpcList;
  private final Integer errorCount;
  private final List<String> delayRpcList;
  private final Integer delayMillis;
  private final Integer delayCount;

  private TestRetryMetricsMiddlewareArgs(
      Logger logger,
      TestRetryMetricsCollector testMetricsCollector,
      String requestId,
      String returnError,
      List<String> errorRpcList,
      Integer errorCount,
      List<String> delayRpcList,
      Integer delayMillis,
      Integer delayCount) {
    this.logger = logger;
    this.testMetricsCollector = testMetricsCollector;
    this.requestId = requestId;
    this.returnError = returnError;
    this.errorRpcList = errorRpcList;
    this.errorCount = errorCount;
    this.delayRpcList = delayRpcList;
    this.delayMillis = delayMillis;
    this.delayCount = delayCount;
  }

  public static class Builder {
    private final Logger logger;
    private final TestRetryMetricsCollector testMetricsCollector;
    private final String requestId;
    private String returnError = null;
    private List<String> errorRpcList = null;
    private Integer errorCount = null;
    private List<String> delayRpcList = null;
    private Integer delayMillis = null;
    private Integer delayCount = null;

    public Builder(
        Logger logger, TestRetryMetricsCollector testMetricsCollector, String requestId) {
      this.logger = logger;
      this.testMetricsCollector = testMetricsCollector;
      this.requestId = requestId;
    }

    public Builder returnError(String returnError) {
      this.returnError = returnError;
      return this;
    }

    public Builder errorRpcList(List<String> errorRpcList) {
      this.errorRpcList = errorRpcList;
      return this;
    }

    public Builder errorCount(Integer errorCount) {
      this.errorCount = errorCount;
      return this;
    }

    public Builder delayRpcList(List<String> delayRpcList) {
      this.delayRpcList = delayRpcList;
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

    public TestRetryMetricsMiddlewareArgs build() {
      return new TestRetryMetricsMiddlewareArgs(
          logger,
          testMetricsCollector,
          requestId,
          returnError,
          errorRpcList,
          errorCount,
          delayRpcList,
          delayMillis,
          delayCount);
    }
  }

  public Logger getLogger() {
    return logger;
  }

  public TestRetryMetricsCollector getTestMetricsCollector() {
    return testMetricsCollector;
  }

  public String getRequestId() {
    return requestId;
  }

  public String getReturnError() {
    return returnError;
  }

  public List<String> getErrorRpcList() {
    return errorRpcList;
  }

  public Integer getErrorCount() {
    return errorCount;
  }

  public List<String> getDelayRpcList() {
    return delayRpcList;
  }

  public Integer getDelayMillis() {
    return delayMillis;
  }

  public Integer getDelayCount() {
    return delayCount;
  }
}
