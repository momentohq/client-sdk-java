package momento.client.example.perf;

import java.util.concurrent.LinkedBlockingQueue;

public class IngestedMembers {

    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue();

    public void add(String member) {
        this.queue.add(member);
    }

    public String remove() throws InterruptedException {
        return this.queue.take();
    }

    public void clear() {
        this.queue.clear();
    }
}
