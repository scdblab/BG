package kosar;

import kosar.TokenObject;


/**
 * The TokenCacheWorker is a worker thread that acquires a semaphore dictating
 * that there is work to be done.
 */
public abstract class TokenWorker extends Thread {
	int workerID = -1;
	private long numRequestsProcessed;

	/**
	 * @param workerID
	 * @param threadId
	 *            the thread id will be added by the {@link ClientBaseAction#threadCount}
	 */
	public TokenWorker(int workerID) {
		this.setName("WorkerID " + workerID);
		this.workerID = workerID;
		numRequestsProcessed = 0;
	}

	public void run() {

		while (AsyncSocketServer.ServerWorking) {
			AsyncSocketServer.startedWorker.incrementAndGet();
			
			try {
				AsyncSocketServer.requestsSemaphores.get(workerID).acquire();

			} catch (InterruptedException e) {
				System.out
						.println("Error: TokenCacheWorker - Could not acquire Semaphore");
				e.printStackTrace();
			}

			// We put this condition boolean here so TokenCacheWorkers
			// may close without performing these actions

			if (AsyncSocketServer.ServerWorking) {
				// Get the token object. Since objects are put in this data
				// structure
				// before the semaphore is released, there should never be a
				// null pointer exception.
				TokenObject tokenObject = null;
				tokenObject = AsyncSocketServer.requestsQueue.get(workerID).poll();
				numRequestsProcessed++;

				if (tokenObject != null) {
					doWork(tokenObject);
				}
			}
		}
	}

	/**
	 * @param socket
	 * @param requestArray
	 */
	public abstract void doWork(TokenObject token);

	public long getNumRequestsProcessed() {
		return numRequestsProcessed;
	}
}