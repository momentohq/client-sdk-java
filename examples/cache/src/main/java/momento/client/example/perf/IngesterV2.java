package momento.client.example.perf;

import com.google.common.collect.Lists;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IngesterV2 {

    private final JedisPool jedisPool;
    private final ExecutorService redisExecutor;
    private final int batchSize;
    private final int numThreads;
    private final AtomicLong totalElements = new AtomicLong(0);
    private final AtomicLong currentSecondWrites = new AtomicLong(0);
    private static final Logger logger = LoggerFactory.getLogger(IngesterV2.class);

    public IngesterV2(JedisPool jedisPool, int batchSize, int numThreads) {
        this.redisExecutor = Executors.newFixedThreadPool(50);
        this.jedisPool = jedisPool;
        this.batchSize = batchSize;
        this.numThreads = numThreads;
    }

    public void ingestSync(final RedisSortedSetEntry sortedSetEntry,
                           boolean readEnabled) {
        String key = sortedSetEntry.getKey();
        final List<RedisSortedSetEntry.MemberScore> memberScores = sortedSetEntry.getMemberScores();
        final List<List<RedisSortedSetEntry.MemberScore>> memberScoresSublists =
                Lists.partition(memberScores, batchSize);
        logger.info(String.format("Total subLists created %d", memberScoresSublists.size()));
        final ProcessedMembers processedMembers = new ProcessedMembers();

        for (List<RedisSortedSetEntry.MemberScore> memberScoreSublist : memberScoresSublists) {
            processedMembers.add(new ProcessedMembers.MemberBatch(memberScoreSublist));
        }
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            logger.info(String.format("{\"key\": \"%s\", \"Throughput-Write\": %d, \"TotalElementsWrote\": %d}",
                    sortedSetEntry.getKey(), currentSecondWrites.getAndSet(0), totalElements.get()));
        }, 1, 1, TimeUnit.SECONDS);

        final Thread[] threads = new Thread[numThreads];
        AtomicInteger batchesProcessed = new AtomicInteger(0);
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                while (processedMembers.hasMembers()) {
                    try (Jedis jedis = jedisPool.getResource()) {
                        final ProcessedMembers.MemberBatch memberBatch =
                                processedMembers.remove();
                        if (memberBatch == null) {
                            logger.info("Thread " + Thread.currentThread().getId() + " done! exiting...");
                            break;
                        };
                        memberBatch.getMemberScoresBatch().forEach(memberScore -> {
                            jedis.zadd(key, memberScore.getScore(), memberScore.getMember());
                            incrementMetrics();
                        });
                        batchesProcessed.getAndIncrement();
                    }
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < numThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        logger.info(String.format("Total batches processed %d", batchesProcessed.get()));

        scheduledExecutorService.shutdownNow();
    }

    private void incrementMetrics() {
        totalElements.incrementAndGet();
        currentSecondWrites.incrementAndGet();
    }
}
