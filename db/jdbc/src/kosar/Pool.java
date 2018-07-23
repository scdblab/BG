package kosar;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

/**
 * A general pool 
 * @author hieun
 *
 */
public abstract class Pool<T> {
	public Semaphore poolSemaphore;
	public ConcurrentLinkedQueue<T> availPool;
	private int initSize = 100;

	public Pool(int size) {
		initSize = size;
		if (null == availPool)
			availPool = new ConcurrentLinkedQueue<T>();
		poolSemaphore = new Semaphore(size);
	}
	
	protected abstract T createItem();

	public void shutdownPool() {
		if (availPool.size() != initSize) {
			System.out
			.println("DB pool size not match the number of DB connections");
			System.exit(0);
		}
		
		availPool = null;
		poolSemaphore = null;
	}

	public T getItem() {
		try {
			boolean result = poolSemaphore.tryAcquire();
			if (!result) {
				synchronized(this.availPool) {
					result = poolSemaphore.tryAcquire();
					if (result) {
						T item = this.availPool.poll();
						if (item == null) {
							System.out.println("Get a null db");
						}
						return item;
					}
					for (int i = 0; i < this.initSize; i++) {
						T item = createItem();
						this.availPool.add(item);
						this.poolSemaphore.release();
					}
					System.out.println(String.format("double connection %d %d %d", this.initSize, this.initSize * 2, this.availPool.size()));
					this.initSize *= 2;
				}
				System.out.println("stuck on sockets!");
				poolSemaphore.acquire();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		T item = this.availPool.poll();
		if (item == null) {
			System.out.println("Get a null db");
		}
		return item;
	}

	public void checkIn(T item) {
		this.availPool.add(item);
		poolSemaphore.release();
	}
}
