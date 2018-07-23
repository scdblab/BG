package kosar;

import edu.usc.bg.base.DB;

/**
 * @author Haoyu Huang
 * 
 * @param <T>
 */
public interface DoReadListener<T> {
	/**
	 * query database
	 * 
	 * @param query
	 * @return the result set
	 */
	public T queryDB(String query);

	/**
	 * cast the value from string to designated type
	 * 
	 * @param value
	 * @return the object casted from string
	 */
	public T cast(Object value);

	public void setUseOnce(boolean useOnce);

	public boolean getUseOnce();

	public void setGetFromDB(boolean queryDB);

	public boolean getFromDB();

	public void incrementRetryCount();

	public int getRetryCount();

	public void incrementRetryOnNoClientHasValue();

	public int getRetryOnNoClientHasValue();

	public int getQueryDBCount();

	public void incrementStealCount();

	public void incrementUseOnceCount();

	public void incrementCopyCount();

	public int getStealFromUserCount();

	public int getUseOnceFromUserCount();

	public int getCopyFromUserCount();

	public void setDB(DB db);

}