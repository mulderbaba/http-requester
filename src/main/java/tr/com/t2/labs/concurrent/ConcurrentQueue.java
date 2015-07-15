package tr.com.t2.labs.concurrent;

import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentQueue<T> {

    private int capacity;
    private AtomicInteger size = new AtomicInteger(0);
    private ArrayDeque<T> arrayDeque;

    private ReentrantLock readLock = new ReentrantLock();
    private AtomicBoolean shutdown = new AtomicBoolean(false);

    protected CountDownLatch shutdownCountDownLatch = new CountDownLatch(1);

    public ConcurrentQueue(int capacity) {
        this.capacity = capacity;
        this.arrayDeque = new ArrayDeque<T>(capacity + 10);
    }

    public T take() {
        readLock.lock();
        try {
            while (true) {
                if (size.get() == 0) {
                    if (shutdown.get()) {
                        return null;
                    } else {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    size.decrementAndGet();
                    return arrayDeque.removeFirst();
                }
            }
        } finally {
            readLock.unlock();
        }
    }

    public void put(T item) {
        while (true) {
            if (size.get() >= capacity) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                addItem(item);
                return;
            }
        }
    }


    protected void addItem(T item) {
        arrayDeque.addLast(item);
        size.incrementAndGet();
    }

    public void onConsumerFinished() {
        shutdownCountDownLatch.countDown();
    }
}
