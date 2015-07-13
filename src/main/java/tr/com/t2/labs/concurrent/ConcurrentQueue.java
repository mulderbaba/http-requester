package tr.com.t2.labs.concurrent;

import java.util.ArrayDeque;
import java.util.Collection;
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

    public T poll() {
        readLock.lock();
        try {
            while (true) {
                if (size.get() == 0) {
                    return null;
                } else {
                    size.decrementAndGet();
                    return arrayDeque.removeFirst();
                }
            }
        } finally {
            readLock.unlock();
        }
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

    public boolean offer(T item, long delta) {
        long start = System.currentTimeMillis();
        while (true) {
            if (size.get() >= capacity) {
                long now = System.currentTimeMillis();
                if (now - start > delta) {
                    return false;
                } else {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                addItem(item);
                return true;
            }
        }
    }

    protected void addItem(T item) {
        arrayDeque.addLast(item);
        size.incrementAndGet();
    }

    public int drainTo(Collection<? super T> collection) {
        readLock.lock();
        try {
            return drainTo(collection, size.get());
        } finally {
            readLock.unlock();
        }
    }

    public int drainTo(Collection<? super T> collection, int maxItemCount) {
        readLock.lock();
        try {
            int len = Math.min(size.get(), maxItemCount);
            if (len > 0) {
                for (int i = 0; i < len; i++) {
                    collection.add(arrayDeque.removeFirst());
                }
                size.addAndGet(-len);
            }
            return len;
        } finally {
            readLock.unlock();
        }
    }

    public void shutdown() {
        shutdown.set(true);

        while (true) {
            if (size.get() > 0) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    shutdownCountDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return;
            }
        }
    }

    public void onConsumerFinished() {
        shutdownCountDownLatch.countDown();
    }

    public int getSize() {
        return size.get();
    }

    public boolean isEmpty() {
        return getSize() == 0;
    }

    public int remainingCapacity() {
        return capacity - getSize();
    }
}
