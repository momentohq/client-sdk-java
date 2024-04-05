package momento.client.example.perf;

import com.google.common.util.concurrent.RateLimiter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

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
    public Reader(final JedisPool jedisPool,
                  final IngestedMembers ingestedMembers,
                  final String sortedSetKey) {
        System.out.println("Reading for sorted set key " + sortedSetKey);
        this.jedisPool = jedisPool;
        this.executorService = Executors.newFixedThreadPool(50); // Pool size can be adjusted as needed
        this.ingestedMembers = ingestedMembers;
        this.key = sortedSetKey;
        scheduleThroughputMeasurement();
        scheduleFillingMembersList();
    }

    private void scheduleFillingMembersList() {
        Thread thread = new Thread(() -> {
            while (true) {
                try {
                    membersToRead.add(ingestedMembers.remove());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();
        threads[0] = thread;
    }

    private void scheduleThroughputMeasurement() {
        scheduledExecutorService.scheduleAtFixedRate(() -> System.out.println(String.format("{\"Throughput-Read\": %d, \"TotalElementsRead\": %d}", currentSecondReads.getAndSet(0), totalElements.get())),
                1, 1, TimeUnit.SECONDS);
    }

    public void start() {
        Thread thread = new Thread(() -> {
            while (true) {
                CompletableFuture<?> done = CompletableFuture.completedFuture(null);
                CompletableFuture<?> batch = CompletableFuture.runAsync(() -> {
                    IntStream.range(0, 10000).forEach(i -> {
                        rateLimiter.acquire();
                        fetchRandomRank(key);
                    });}
                , executorService);
                done.thenCombine(batch, (aVoid, aVoid2) -> null);
            }
        });
        thread.start();
        threads[1] = thread;
    }

    public void fetchRandomRank(final String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            int start = ThreadLocalRandom.current().nextInt(0, 200000);
            List<String> members = jedis.zrange(key, start, start + 200);

            if (!members.isEmpty()) {
                currentSecondReads.getAndIncrement();
                totalElements.getAndAdd(members.size());
            }
        }
    }

    public void shutdown() {
        try {
            threads[0].join();
            threads[1].join();
            scheduledExecutorService.shutdownNow();
            executorService.shutdownNow();
        } catch (InterruptedException e) {

        }
    }

    // to read from ad-hoc leaderboards
    public static void main(String... args) {
        final JedisPool readerPool = new JedisPool("localhost", 6666);
        final Reader reader = new Reader(readerPool, new IngestedMembers(),
                "fqquafa5c|_lb|lb:keystone:region-3:faction-horde:strict:season-sl-1:faction-horde");
        reader.start();
    }
}
