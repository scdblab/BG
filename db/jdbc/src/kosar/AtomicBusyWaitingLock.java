package kosar;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class AtomicBusyWaitingLock {
	AtomicBoolean atomicBoolean = new AtomicBoolean(false);
	
	Semaphore lock = new Semaphore(1);
	
	boolean buzyWaiting=false;
	
	public void lock() {
		if (buzyWaiting) {
			while (!this.atomicBoolean.compareAndSet(false, true));
		} else {
			try {
				lock.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void unlock() {
		if (buzyWaiting) {
			this.atomicBoolean.set(false);
		} else {
			lock.release();
		}
	}
}
