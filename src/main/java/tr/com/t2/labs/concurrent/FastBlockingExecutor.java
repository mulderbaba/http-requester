package tr.com.t2.labs.concurrent;

import java.util.concurrent.Executor;


public class FastBlockingExecutor implements Executor {

	private static final int DEFAULT_THREAD_COUNT = 1;
	private static final int DEFAULT_QUEUE_SIZE = 5;
	
	private ConcurrentQueue<Runnable> concurrentQueue;
	
	public FastBlockingExecutor(String name) {
		this(name, DEFAULT_QUEUE_SIZE, DEFAULT_THREAD_COUNT);
	}

	public FastBlockingExecutor(String name, int queueSize, int threadCount) {
		concurrentQueue = new ConcurrentQueue<Runnable>(queueSize);
		new ConcurrentQueueConsumerThreadPool(name, concurrentQueue, threadCount);
	}
	
	@Override
	public void execute(Runnable command) {
		concurrentQueue.put(command);
	}
}
