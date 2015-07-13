package tr.com.t2.labs.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentQueueConsumerThreadPool {

	private ConcurrentQueue<? extends Runnable> queue;
	private TaskWorker[] workerThreadArray;

	private AtomicInteger finishedWorkerCount = new AtomicInteger(0);

	public ConcurrentQueueConsumerThreadPool(final String name, ConcurrentQueue<? extends Runnable> queue, final int poolSize) {
		this(name, queue, poolSize, Thread.NORM_PRIORITY);
	}

	public ConcurrentQueueConsumerThreadPool(final String name, ConcurrentQueue<? extends Runnable> queue, final int poolSize, final int threadPriority) {
		this.queue = queue;

		workerThreadArray = new TaskWorker[poolSize];
		new Thread(new Runnable() {
			public void run() {
				for(int i=0; i<poolSize; i++) {
					workerThreadArray[i] = new TaskWorker();
					workerThreadArray[i].setName("CQCThreadPool: " + i);
					workerThreadArray[i].setPriority(threadPriority);
					workerThreadArray[i].start();
				}
			}
		}, "CQCThreadPool: " + name + " initiliazer thread").start();
	}

	private void onWorkerFinished() {
		int count = finishedWorkerCount.incrementAndGet();
		if ( workerThreadArray.length == count ) {
			queue.onConsumerFinished();
		}
	}

	private class TaskWorker extends Thread {

		public void run() {
			Runnable runnable = queue.take();
			while ( runnable != null ) {

				runnable.run();
				runnable = queue.take();
			}

			onWorkerFinished();
		}
	}
}
