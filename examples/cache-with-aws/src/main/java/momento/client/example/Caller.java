package momento.client.example;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.PredefinedClientConfigurations;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class Caller {

    private static final Random random = new Random();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
            .withRegion("us-west-2")
            .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
            .withClientConfiguration(PredefinedClientConfigurations.defaultConfig().withMaxConnections(300))
            .build();
    private static final String QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/616729109836/momento-cdt";

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(300); // Thread pool of 500

        String[] users = IntStream.range(1, 21).mapToObj(i -> "user" + i).toArray(String[]::new);
        String[] metrics = IntStream.range(1, 101).mapToObj(i -> "metric" + i).toArray(String[]::new);

        AtomicInteger messagesSent = new AtomicInteger(0);

        while (true) {

            executor.submit(() -> {
                String tntid = getRandomElement(users);
                String metricId = getRandomElement(metrics); // Now sending one metric per message
                sendMessageToSQS(tntid, metricId);
            });
        }
    }

    private static void sendMessageToSQS(String tntid, String metricId) {
        try {
            SQSPayload payload = new SQSPayload(tntid, metricId);
            String messageBody = objectMapper.writeValueAsString(payload);
            SendMessageRequest send_msg_request = new SendMessageRequest()
                    .withQueueUrl(QUEUE_URL)
                    .withMessageBody(messageBody);
            sqsClient.sendMessage(send_msg_request);
            //System.out.println("Message sent to SQS with tntid: " + tntid + " and metricId: " + metricId);
        } catch (Exception e) {
            System.err.println("Error sending message to SQS: " + e.getMessage());
        }
    }

    private static String getRandomElement(String[] array) {
        return array[random.nextInt(array.length)];
    }

    private static class SQSPayload {
        public String tntid;
        public String metricId;

        public SQSPayload(String tntid, String metricId) {
            this.tntid = tntid;
            this.metricId = metricId;
        }
    }
}
