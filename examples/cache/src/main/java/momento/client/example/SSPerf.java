package momento.client.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SSPerf {

    public static class RedisSortedSetEntry {
        private String key;
        private Map<String, Double> memberScores;

        public RedisSortedSetEntry(String key, Map<String, Double> memberScores) {
            this.key = key;
            this.memberScores = memberScores;
        }

        public String getKey() {
            return key;
        }

        public Map<String, Double> getMemberScores() {
            return memberScores;
        }
    }
    private static final AtomicLong numElementsInserted = new AtomicLong(0);
    private static final AtomicLong operationsPerSecond = new AtomicLong(0);

    public static void main(String[] args) {
        String redisHost = "localhost";
        int redisPort = 6666;
        String filePath = "/Users/pratik/sandbox/leaderboard_analysis/leaderboard_500.jsonl";

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.scheduleAtFixedRate(() -> {
            long ops = operationsPerSecond.getAndSet(0);
            long totalOps = numElementsInserted.get();
            System.out.println("{\"operations\":" + ops + ", \"numElementsInserted\":" + totalOps + "}");
        }, 1, 1, TimeUnit.SECONDS);

        final ExecutorService redisExecutor = Executors.newFixedThreadPool(50);

        // Initialize JedisPool
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort);

        try {
            List<String> allLines = Files.readAllLines(new File(filePath).toPath());
            System.out.println("Loaded all lines into memory.");

            ObjectMapper objectMapper = new ObjectMapper();
            List<RedisSortedSetEntry> entries = new ArrayList<>();

            // Pre-parse JSON lines
            for (String line : allLines) {
                JsonNode rootNode = objectMapper.readTree(line);
                String key = rootNode.get("key").asText();
                JsonNode valuesNode = rootNode.get("value");

                Map<String, Double> memberScores = new HashMap<>();
                valuesNode.fields().forEachRemaining(entry -> memberScores.put(entry.getKey(), entry.getValue().asDouble()));
                entries.add(new RedisSortedSetEntry(key, memberScores));
            }

            // Process and write to Redis using Jedis instances from the pool
            for (RedisSortedSetEntry entry : entries) {
                entry.getMemberScores().forEach((member, score) -> {
                    redisExecutor.submit(() -> {
                        try (Jedis jedis = jedisPool.getResource()) {
                            jedis.zadd(entry.getKey(), score, member);
                            numElementsInserted.incrementAndGet();
                            operationsPerSecond.incrementAndGet();
                        }
                    });
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            redisExecutor.shutdown(); // Initiate shutdown
            try {
                // Wait a certain time for the termination of previously submitted tasks
                if (!redisExecutor.awaitTermination(200, TimeUnit.MINUTES)) { // Adjust time as needed
                    redisExecutor.shutdownNow(); // Cancel currently executing tasks
                    // Wait again, if necessary
                    if (!redisExecutor.awaitTermination(200, TimeUnit.MINUTES))
                        System.err.println("Redis executor did not terminate.");
                }
            } catch (InterruptedException ie) {
                // (Re-)Cancel if current thread also interrupted
                redisExecutor.shutdownNow();
                // Preserve interrupt status
                Thread.currentThread().interrupt();
            }
            executorService.shutdown();
            jedisPool.close(); // Close the pool when done
        }
    }
}
