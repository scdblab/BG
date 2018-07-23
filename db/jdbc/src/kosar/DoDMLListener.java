package kosar;

import java.util.Set;
import edu.usc.bg.base.DB;

/**
 * @author Haoyu Huang
 * 
 */
public interface DoDMLListener {
	/**
	 * used for test only
	 * 
	 * @param dml
	 * @return
	 */
	public Set<String> getInternalKeys(String dml);

	/**
	 * update the database
	 * 
	 * @param dml
	 * @return 0 if transaction committed, -1 otherwise
	 */
	public int updateDB(String dml);

	public void setDB(DB db);
}