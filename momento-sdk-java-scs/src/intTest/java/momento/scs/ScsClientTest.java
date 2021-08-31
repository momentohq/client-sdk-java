/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package momento.scs;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.momento.scs.ClientGetResponse;
import org.momento.scs.ClientSetResponse;
import org.momento.scs.MomentoResult;
import org.momento.scs.ScsClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


class ScsClientTest {
    ScsClient c;

    @BeforeAll
    static void beforeAll() {
        if (System.getenv("TEST_AUTH_TOKEN") == null) {
            throw new IllegalArgumentException("Integration tests require TEST_AUTH_TOKEN env var; see README for more details.");
        }
    }

    @BeforeEach
    void setUp() {
        c = getScsClient(Optional.empty());
    }

    ScsClient getScsClient(Optional<OpenTelemetry> openTelemetry) {
        return getScsClient(System.getenv("TEST_AUTH_TOKEN"), openTelemetry);
    }

    ScsClient getScsClient(String authToken, Optional<OpenTelemetry> openTelemetry) {
        String endpoint = System.getenv("TEST_ENDPOINT");
        if (endpoint == null) {
            endpoint = "alpha.cacheservice.com";
        }

        return new ScsClient(authToken, openTelemetry, endpoint, System.getenv("TEST_SSL_INSECURE") != null);
    }

    @AfterEach
    void tearDown() throws Exception{
        stopIntegrationTestOtel();
    }

    @Test
    void testBlockingClientHappyPath() {
        testHappyPath(c);
    }

    @Test
    void testBlockingClientHappyPathWithTracing() throws Exception{
        startIntegrationTestOtel();
        OpenTelemetrySdk openTelemetry = setOtelSDK();
        ScsClient client = getScsClient(Optional.of(openTelemetry));
        testHappyPath(client);
        // To accommodate for delays in tracing logs to appear in docker
        Thread.sleep(1000);
        verifySetTrace("1");
        verifyGetTrace("1");
    }

    void testHappyPath (ScsClient scsClient) {
        try {
            String key = UUID.randomUUID().toString();

            //Set Key sync
            ClientSetResponse setRsp = scsClient.set(
                    key,
                    ByteBuffer.wrap("bar".getBytes(StandardCharsets.UTF_8)),
                    2
            );
            Assertions.assertEquals(MomentoResult.Ok, setRsp.getResult());

            // Get Key that was just set
            ClientGetResponse<ByteBuffer> rsp = scsClient.get(key);

            Assertions.assertEquals(MomentoResult.Hit, rsp.getResult());
            Assertions.assertEquals("bar", StandardCharsets.US_ASCII.decode(rsp.getBody()).toString());

        } catch (IOException e) {
            Assertions.fail(e);
        }
    }

    @Test
    void testAsyncClientHappyPath() {
        testAsyncHappyPath(c);
    }

    @Test
    void testAsyncClientHappyPathWithTracing() throws Exception{
        startIntegrationTestOtel();
        OpenTelemetrySdk openTelemetry = setOtelSDK();
        ScsClient client = getScsClient(Optional.of(openTelemetry));
        testAsyncHappyPath(client);
        // To accommodate for delays in tracing logs to appear in docker
        Thread.sleep(1000);
        verifySetTrace("1");
        verifyGetTrace("1");
    }

    void testAsyncHappyPath(ScsClient client) {
        try {
            String key = UUID.randomUUID().toString();
            // Set Key Async
            CompletionStage<ClientSetResponse> setRsp = client.setAsync(
                    key,
                    ByteBuffer.wrap("bar".getBytes(StandardCharsets.UTF_8)),
                    10
            );
            Assertions.assertEquals(MomentoResult.Ok, setRsp.toCompletableFuture().get().getResult());

            // Get Key Async
            ClientGetResponse<ByteBuffer> rsp = client.getAsync(key).toCompletableFuture().get();

            Assertions.assertEquals(MomentoResult.Hit, rsp.getResult());
            Assertions.assertEquals("bar", StandardCharsets.US_ASCII.decode(rsp.getBody()).toString());

        } catch (IOException | InterruptedException | ExecutionException e) {
            Assertions.fail(e);
        }
    }
    @Test
    void testTtlHappyPath() {
        testTtlHappyPath(c);
    }

    @Test
    void testTtlHappyPathWithTracing() throws Exception {
        startIntegrationTestOtel();
        OpenTelemetrySdk openTelemetry = setOtelSDK();
        ScsClient client = getScsClient(Optional.of(openTelemetry));
        testTtlHappyPath(client);
        // To accommodate for delays in tracing logs to appear in docker
        Thread.sleep(1000);
        verifySetTrace("1");
        verifyGetTrace("1");
    }

    void testTtlHappyPath(ScsClient client) {
        try {
            String key = UUID.randomUUID().toString();

            //Set Key sync
            ClientSetResponse setRsp = client.set(
                    key,
                    ByteBuffer.wrap("bar".getBytes(StandardCharsets.UTF_8)),
                    1
            );
            Assertions.assertEquals(MomentoResult.Ok, setRsp.getResult());

            Thread.sleep(1500);

            // Get Key that was just set
            ClientGetResponse<ByteBuffer> rsp = client.get(key);

            Assertions.assertEquals(MomentoResult.Miss, rsp.getResult());

        } catch (IOException | InterruptedException e) {
            Assertions.fail(e);
        }
    }

    @Test
    void testMissHappyPath() {
        testMissHappyPathInternal(c);
    }

    @Test
    void testMissHappyPathWithTracing() throws Exception{
        startIntegrationTestOtel();
        OpenTelemetrySdk openTelemetry = setOtelSDK();
        ScsClient client = getScsClient(Optional.of(openTelemetry));
        testMissHappyPathInternal(client);
        // To accommodate for delays in tracing logs to appear in docker
        Thread.sleep(1000);
        verifySetTrace("0");
        verifyGetTrace("1");
    }

    void testMissHappyPathInternal(ScsClient client) {
        try {
            // Get Key that was just set
            ClientGetResponse<ByteBuffer> rsp = client.get(UUID.randomUUID().toString());

            Assertions.assertEquals(MomentoResult.Miss, rsp.getResult());

        } catch (IOException e) {
            Assertions.fail(e);
        }
    }


    @Test
    void testBadAuthToken() {
        ScsClient badCredClient = getScsClient("BAD_TOKEN", Optional.empty());
        testBadAuthToken(badCredClient);
    }

    @Test
    void testBadAuthTokenWithTracing() throws Exception{
        startIntegrationTestOtel();
        OpenTelemetrySdk openTelemetry = setOtelSDK();
        ScsClient client = getScsClient("BAD_TOKEN", Optional.of(openTelemetry));
        testBadAuthToken(client);
        // To accommodate for delays in tracing logs to appear in docker
        Thread.sleep(1000);
        verifySetTrace("0");
        verifyGetTrace("1");
    }

    void testBadAuthToken(ScsClient badCredClient) {

        try {
            // Get Key that was just set
            ClientGetResponse<ByteBuffer> rsp = badCredClient.get(UUID.randomUUID().toString());

            Assertions.fail("expected PERMISSION_DENIED io.grpc.StatusRuntimeException");

        } catch (IOException e) {
            Assertions.fail(e);
        } catch (io.grpc.StatusRuntimeException e) {

            // Make sure we get permission denied error the way we would expected
            Assertions.assertEquals(
                    new StatusRuntimeException(
                            Status.PERMISSION_DENIED
                                    .withDescription("Malformed authorization token")
                    ).toString(),
                    e.toString()
            );
        }
    }

    /**
     * ================ HELPER FUNCTIONS ======================================
     */
    OpenTelemetrySdk setOtelSDK() {
        String otelGwUrl = "0.0.0.0";
        // this is due to the cfn export format we are using for the vpc endpoints
        String serviceUrl = "http://" + otelGwUrl + ":" + "4317";
        OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(serviceUrl)
                .setTimeout(2, TimeUnit.SECONDS)
                .build();
        BatchSpanProcessor spanProcessor = BatchSpanProcessor.builder(spanExporter)
                .setScheduleDelay(1, TimeUnit.MILLISECONDS)
                .build();
        SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .build();
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
        Runtime.getRuntime().addShutdownHook(new Thread(sdkTracerProvider::shutdown));
        return openTelemetry;
    }

    void startIntegrationTestOtel() throws Exception {
        stopIntegrationTestOtel();
        shellex( "docker run -p 4317:4317 --name integtest_otelcol " +
                        "    -v ${PWD}/otel-collector-config.yaml:/etc/otel-collector-config.yaml " +
                        "    -d otel/opentelemetry-collector:latest " +
                        "    --config=/etc/otel-collector-config.yaml",
                true,
                "started local otel container for integration testing",
                "failed to bootstrap integration test local otel container");
        shellExpect( 5,
                "docker logs integtest_otelcol 2>& 1",
                ".*Everything is ready. Begin running and processing data.*");

    }

    void stopIntegrationTestOtel() throws Exception{
        shellex("docker stop integtest_otelcol && docker rm integtest_otelcol",
                false,
                "successfully stopped otelcol test container",
                "it is okay for this to fail because maybe it is not present");

    }

    void verifySetTrace(String expectedCount) throws Exception{
        String count = shellex("docker logs   integtest_otelcol 2>& 1 | grep  Name | grep java-sdk-set-request | wc -l",
                true,
                "Verify set trace",
                "failed to verify set trace"
                );
        Assertions.assertEquals(expectedCount, count.trim());
    }

    void verifyGetTrace(String expectedCount) throws Exception{
        String count = shellex("docker logs   integtest_otelcol 2>& 1 | grep  Name | grep java-sdk-get-request | wc -l",
                true,
                "Verify get trace",
                "failed to verify get trace"
        );
        Assertions.assertEquals(expectedCount, count.trim());
    }

    // Polls a command until the expected result comes back
    private void shellExpect(double timeoutSeconds, String command, String outputRegex) throws Exception {
        long start = System.currentTimeMillis();
        String lastOutput = "";

        while ((System.currentTimeMillis() - start) / 1000.0 < timeoutSeconds ) {
            lastOutput = shellex(command, false, null, null);
            if (lastOutput.matches(outputRegex)) {
                return;
            }
        }

        throw new InternalError(String.format("Never got expected output. Last:\n%s", lastOutput));
    }

    private String shellex(String command, Boolean expectSuccess, String successDescription, String failDescription) throws Exception{
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("sh", "-c", command);
        Process process = processBuilder.start();

        int exitCode = process.waitFor();

       String output = "";
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {

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