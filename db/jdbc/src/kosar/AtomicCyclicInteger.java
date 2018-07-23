package kosar;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicCyclicInteger extends AtomicInteger {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int limit;
	
	public AtomicCyclicInteger(int limit) {
		this.limit = limit;
	}
	
	/**
	 * Atomically increments by one the current value. If the value reaches
	 * Integer.MAX_VALUE, it will set back to 0.
	 * 
	 * @return the updated value
	 */
	public final int incrementCycleAndGet() {
		for (;;) {
			int current = get();
			int next = -1;
			if (current == this.limit) {
				next = 0;
			} else {
				next = current + 1;
			}
			if (compareAndSet(current, next))
				return next;
		}
	}
}
