package momento.client.example.perf;

import org.checkerframework.checker.units.qual.A;

import java.util.concurrent.atomic.AtomicInteger;

public class PeriodicMetrics {
    private final AtomicInteger numElementsInserted = new AtomicInteger(0);
    private final AtomicInteger totalElements = new AtomicInteger(0);
}


//
//    ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
//    executorService.scheduleAtFixedRate(
//        () -> {
//          long ops = operationsPerSecond.getAndSet(0);
//          long totalOps = numElementsInserted.get();
//          System.out.println(
//              "{\"operations\":" + ops + ", \"numElementsInserted\":" + totalOps + "}");
//        },
//        1,
//        1,
//        TimeUnit.SECONDS);
