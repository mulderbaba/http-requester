package tr.com.t2.labs.concurrent;

import java.util.concurrent.Executor;


public class FastBlockingExecutor implements Executor {

	private ConcurrentQueue<Runnable> concurrentQueue;
	
	public FastBlockingExecutor(String name, int queueSize, int threadCount) {
		concurrentQueue = new ConcurrentQueue<Runnable>(queueSize);
		new ConcurrentQueueConsumerThreadPool(name, concurrentQueue, threadCount);
	}
	
	@Override
	public void execute(Runnable command) {
		concurrentQueue.put(command);
	}
}
