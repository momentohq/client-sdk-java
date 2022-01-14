package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

final class OtelTestHelpers {

  private OtelTestHelpers() {}

  static OpenTelemetrySdk setOtelSDK() {
    String otelGwUrl = "0.0.0.0";
    // this is due to the cfn export format we are using for the vpc endpoints
    String serviceUrl = "http://" + otelGwUrl + ":" + "4317";
    OtlpGrpcSpanExporter spanExporter =
        OtlpGrpcSpanExporter.builder()
            .setEndpoint(serviceUrl)
            .setTimeout(2, TimeUnit.SECONDS)
            .build();
    BatchSpanProcessor spanProcessor =
        BatchSpanProcessor.builder(spanExporter).setScheduleDelay(1, TimeUnit.MILLISECONDS).build();
    SdkTracerProvider sdkTracerProvider =
        SdkTracerProvider.builder().addSpanProcessor(spanProcessor).build();
    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    Runtime.getRuntime().addShutdownHook(new Thread(sdkTracerProvider::shutdown));
    return openTelemetry;
  }

  static void startIntegrationTestOtel() throws Exception {
    stopIntegrationTestOtel();
    shellex(
        "docker run -p 4317:4317 --name integtest_otelcol "
            + "    -v ${PWD}/otel-collector-config.yaml:/etc/otel-collector-config.yaml "
            + "    -d otel/opentelemetry-collector:latest "
            + "    --config=/etc/otel-collector-config.yaml",
        true,
        "started local otel container for integration testing",
        "failed to bootstrap integration test local otel container");
    shellExpect(
        5,
        "docker logs integtest_otelcol 2>& 1",
        ".*Everything is ready. Begin running and processing data.*");
  }

  static void stopIntegrationTestOtel() throws Exception {
    shellex(
        "docker stop integtest_otelcol && docker rm integtest_otelcol",
        false,
        "successfully stopped otelcol test container",
        "it is okay for this to fail because maybe it is not present");
  }

  static void verifySetTrace(String expectedCount) throws Exception {
    String count =
        shellex(
            "docker logs   integtest_otelcol 2>& 1 | grep  Name | grep java-sdk-set-request | wc -l",
            true,
            "Verify set trace",
            "failed to verify set trace");
    assertEquals(expectedCount, count.trim());
  }

  static void verifyGetTrace(String expectedCount) throws Exception {
    String count =
        shellex(
            "docker logs   integtest_otelcol 2>& 1 | grep  Name | grep java-sdk-get-request | wc -l",
            true,
            "Verify get trace",
            "failed to verify get trace");
    assertEquals(expectedCount, count.trim());
  }

  // Polls a command until the expected result comes back
  private static void shellExpect(double timeoutSeconds, String command, String outputRegex)
      throws Exception {
    long start = System.currentTimeMillis();
    String lastOutput = "";

    while ((System.currentTimeMillis() - start) / 1000.0 < timeoutSeconds) {
      lastOutput = shellex(command, false, null, null);
      if (lastOutput.matches(outputRegex)) {
        return;
      }
    }

    throw new InternalError(String.format("Never got expected output. Last:\n%s", lastOutput));
  }

  private static String shellex(
      String command, Boolean expectSuccess, String successDescription, String failDescription)
      throws Exception {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.command("sh", "-c", command);
    Process process = processBuilder.start();

    int exitCode = process.waitFor();

    String output = "";
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {

      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
        output += line;
      }
    }

    if (exitCode == 0) {
      if (successDescription != null) {
        System.out.println(successDescription);
      }
    } else if (expectSuccess) {
      throw new InternalError(failDescription + "exit_code:" + exitCode);
    } else {
      if (failDescription != null) {
        System.out.println(failDescription);
      }
    }

    return output;
  }
}
