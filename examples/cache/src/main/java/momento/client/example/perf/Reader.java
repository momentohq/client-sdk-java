package momento.client.example.perf;

import com.google.common.util.concurrent.RateLimiter;
import org.checkerframework.checker.units.qual.A;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPoolConfig;

public class Reader {

    private final IngestedMembers ingestedMembers;
    private final JedisPool jedisPool;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
    private final AtomicLong totalElements = new AtomicLong(0);
    private final AtomicLong currentSecondReads = new AtomicLong(0);
    private final List<String> membersToRead = new ArrayList<>();
    private final RateLimiter rateLimiter = RateLimiter.create(200);

    private final String key;

    private Thread[] threads = new Thread[2];
    private static final Logger logger = LoggerFactory.getLogger(Reader.class);

    public Reader(final JedisPool jedisPool,
                  final IngestedMembers ingestedMembers,
                  final String sortedSetKey) {
        logger.info("Reading for sorted set key " + sortedSetKey);
        this.jedisPool = jedisPool;
        this.executorService = Executors.newFixedThreadPool(50); // Pool size can be adjusted as needed
        this.ingestedMembers = ingestedMembers;
        this.key = sortedSetKey;
        scheduleFillingMembersList();
        scheduleThroughputMeasurement();
    }

    private void scheduleFillingMembersList() {
        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    membersToRead.add(ingestedMembers.remove());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        threads[0] = thread;
        thread.start();
    }

    private void scheduleThroughputMeasurement() {
        scheduledExecutorService.scheduleAtFixedRate(() -> logger.info(String.format("{\"Throughput-Read\": %d, \"TotalElementsRead\": %d}", currentSecondReads.getAndSet(0), totalElements.get())),
                1, 1, TimeUnit.SECONDS);
    }

    public void start(Optional<Integer> totalLeaderboradEntries) {

        Thread thread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                if (membersToRead.isEmpty()) continue;
                rateLimiter.acquire();
                fetchRandomRank(key, totalLeaderboradEntries);
            }
        });
        this.threads[1] = thread;
        thread.start();
    }


    public void fetchRandomRank(final String key, final Optional<Integer> totalLeaderboardEntries) {
        try {
            try (Jedis jedis = jedisPool.getResource()) {
                int start = ThreadLocalRandom.current().nextInt(0, totalLeaderboardEntries.orElseGet(membersToRead::size));
                int stop = totalLeaderboardEntries.orElseGet(() -> start + Math.min(100, membersToRead.size()));
                long startTime = System.nanoTime();
                List<String> members;
                try {
                    members = jedis.zrange(key, start, stop);
                } catch (Exception e) {
                    logger.error("Received exception from Redis " + e);
                    return;
                }
                long duration = System.nanoTime() - startTime;

                if (!members.isEmpty()) {
                    currentSecondReads.getAndIncrement();
                    totalElements.getAndIncrement();
                    long timestampEpoch = System.currentTimeMillis();
                    String json = String.format("{\"key\": \"%s\", \"duration\": %d, \"timestampEpoch\": %d,\"totalMembers\": %d}\n", key, duration, timestampEpoch, members.size());

                    logger.info(json);
                }
            else {
                System.out.println("No more data");
                this.shutdown();
            }
            }
        } catch (Exception e) {
            logger.error("Caught exception: " + e);
        }
    }

    public void shutdown() {
        threads[0].interrupt();
        threads[1].interrupt();
        scheduledExecutorService.shutdownNow();
        executorService.shutdownNow();
    }

    // to read from ad-hoc leaderboards
    public static void main(String... args) {
        // Check if an argument (file path) is provided
        if (args.length < 1) {
            logger.info("Usage: ./gradlew reader --args <key>");
            System.exit(1);
        }

        final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        final JedisPool readerPool = new JedisPool(new JedisPoolConfig(), "localhost", 6666);
        final Reader reader = new Reader(readerPool, new IngestedMembers(), args[0]);

        int totalEntries = 1_000_000;
        if (args.length > 1) {
            totalEntries = Integer.parseInt(args[1]);
        }

        System.out.println("Starting to read...!");
        // have to know approx total entries in case of an ad-hoc run to read a leaderboard to perform
        // random rank range reads.
        reader.start(Optional.of(totalEntries));
    }
}
