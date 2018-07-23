//package kosar;
//
//import java.util.concurrent.Semaphore;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import edu.usc.bg.base.DB;
//
//public class DBJob {
//	private SimpleDoDMLListener dmlListener;
//	private SimpleDoReadListener readListener;
//	private String query;
//	private Object result;
//	
//	private Semaphore latch;
//	private AtomicInteger count;
//	
//	public DBJob(Object listener, String query, 
//			Semaphore latch, AtomicInteger count) {
//		this.query = query;
//		this.latch = latch;
//		this.count = count;
//		this.dmlListener = new SimpleDoDMLListener(null, null);
//		this.readListener = new SimpleDoReadListener(null);
//	}
//
//	public void execute(DB db) {
//		switch (query.charAt(0)) {
//		case CoreClient.INVITE:
//		case CoreClient.ACCEPT:
//		case CoreClient.REJECT:
//		case CoreClient.THAW:
//			int ret = -1;
//			while (ret != 0) {
//				dmlListener.setDB(db);
//				ret = dmlListener.updateDB(query);
//				if (ret != 0)
//					ClientAction.updateFail.incrementAndGet();
//				break;
//			}
//			break;
//		case CoreClient.VIEW_PROFILE:
//		case CoreClient.VIEW_PENDING:
//		case CoreClient.LIST_FRIEND:
//			readListener = new SimpleDoReadListener(db);
//			result = readListener.queryDB(query);
//			break;
//		}		
//	}
//	
//	public Object getResult() {
//		return result;
//	}
//	
//	public String getQuery() {
//		return query;
//	}
//
//	public void finish() {
//		if (count != null) {
//			if (count.decrementAndGet() == 0) {
//				latch.release();
//			}		
//		}
//	}
//}
