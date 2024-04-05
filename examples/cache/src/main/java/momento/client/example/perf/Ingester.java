package momento.client.example.perf;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Ingester {

    private final JedisPool jedisPool;
    private final ExecutorService redisExecutor;
    private final int batchSize;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final AtomicLong totalElements = new AtomicLong(0);
    private final AtomicLong currentSecondWrites = new AtomicLong(0);
    public Ingester(final JedisPool jedisPool, int poolSize, int batchSize) {
        this.jedisPool = jedisPool;
        this.redisExecutor = Executors.newFixedThreadPool(poolSize);
        this.batchSize = batchSize;
        scheduleThroughputMeasurement();
    }

    private void scheduleThroughputMeasurement() {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            System.out.println(String.format("{\"Throughput-Write\": %d, \"TotalElementsWrote\": %d}", currentSecondWrites.getAndSet(0), totalElements.get()));
        }, 1, 1, TimeUnit.SECONDS);
    }

    public CompletableFuture<Void> ingestAsync(final RedisSortedSetEntry sortedSetEntry) {
        String key = sortedSetEntry.getKey();
        Map<String, Double> memberScores = sortedSetEntry.getMemberScores();
        List<Map.Entry<String, Double>> entries = new ArrayList<>(memberScores.entrySet());

        CompletableFuture<Void> allDoneFuture = CompletableFuture.completedFuture(null);
        final IngestedMembers ingestedMembers = new IngestedMembers();
        Reader reader = null;
//        if (sortedSetEntry.getMemberScores().size() > 1000) {
//            final JedisPool readerPool = new JedisPool("localhost", 6666);
//            reader = new Reader(readerPool, ingestedMembers, key);
//            reader.start();
//        }

        for (int i = 0; i < entries.size(); i += batchSize) {
            final int batchStart = i;
            final int batchEnd = Math.min(batchStart + batchSize, entries.size());
            List<Map.Entry<String, Double>> batch = entries.subList(batchStart, batchEnd);

            CompletableFuture<Void> batchFuture = CompletableFuture.runAsync(() -> {
                try (Jedis jedis = jedisPool.getResource()) {
                    for (Map.Entry<String, Double> entry : batch) {
                        jedis.zadd(key, entry.getValue(), entry.getKey());
                        ingestedMembers.add(entry.getKey());
                        incrementMetrics();
                    }
                }
            }, redisExecutor);

            allDoneFuture = allDoneFuture.thenCombine(batchFuture, (aVoid, aVoid2) -> null);
        }

        if (reader != null) reader.shutdown();
        return allDoneFuture;
    }

    private void incrementMetrics() {
        totalElements.incrementAndGet();
        currentSecondWrites.incrementAndGet();
    }

    public void shutdown() {
        redisExecutor.shutdown();
        scheduledExecutorService.shutdown();
    }

    // to ingest ad-hoc leaderboard
    public static void main(String... args) throws Exception {
        // Check if an argument (file path) is provided
        if (args.length < 1) {
            System.out.println("Usage: java ingester <filePath>");
            System.exit(1);
        }

        final String filePath = args[0];
        final List<String> jsonLines = LeaderboardJSONLReader.parseFile(filePath);

        Ingester ingester = new Ingester(new JedisPool("localhost", 6666),
                50, 100000);
        jsonLines.forEach(jsonLine -> {
            final RedisSortedSetEntry sortedSetEntry = SortedSetLineProcessor.processLine(
                    jsonLine
            );
            try {
                ingester.ingestAsync(sortedSetEntry).get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

    }

}
