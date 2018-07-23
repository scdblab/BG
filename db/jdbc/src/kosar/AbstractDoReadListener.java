package kosar;

/**
 * An abstract Read listener
 * 
 * @author Haoyu Huang
 *
 * @param <T>
 */
public abstract class AbstractDoReadListener<T> implements DoReadListener<T> {

	private boolean useOnce; 
	
	private int retryCount;
	
	private int readDBCount;
	
	private int stealCount;
	
	private int copyCount;
	
	private int useOnceCount;
	
	private boolean getFromDB;
	
	private int retryOnNoClientCount;
	
	public T queryDB(String query) {
		this.readDBCount++;
		return null;
	}
	
	public void setGetFromDB(boolean queryDB) {
		this.getFromDB = queryDB;
	}
	
	public boolean getFromDB() {
		return this.getFromDB;
	}
	
	public void setUseOnce(boolean useOnce) {
		this.useOnce = useOnce;
	}

	public boolean getUseOnce() {
		return this.useOnce;
	}

	public void incrementRetryCount() {
		this.retryCount++;
	}
	
	public int getRetryCount() {
		return this.retryCount;
	}
	
	public int getQueryDBCount() {
		return this.readDBCount;
	}
	
	public void incrementStealCount() {
		this.stealCount++;
	}
	
	public void incrementUseOnceCount() {
		this.useOnceCount++;
	}
	
	public void incrementCopyCount() {
		this.copyCount++;
	}
	
	public void incrementRetryOnNoClientHasValue() {
		this.retryOnNoClientCount++;
	}
	
	public int getRetryOnNoClientHasValue() {
		return this.retryOnNoClientCount;
	}
	
	public int getStealFromUserCount() {
		return this.stealCount;
	}
	
	public int getUseOnceFromUserCount() {
		return this.useOnceCount;
	}
	
	public int getCopyFromUserCount() {
		return this.copyCount;
	}
}
