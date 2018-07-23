//package microbench;
//
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Random;
//import java.util.Set;
//import java.util.Vector;
//import java.util.concurrent.Semaphore;
//
//import kosar.ClientAction;
//import kosar.CoreClient;
//import kosar.RelaxedConsistencyClient;
//import edu.usc.bg.base.ByteIterator;
//import edu.usc.bg.base.DB;
//import edu.usc.bg.base.DBException;
//
//public class SendUpdateTest {
//	DB db;
//	
//	public static final int TIME_LOOP = 1000000;
//	
//	/** The class to use as the jdbc driver. */
//	public static final String DRIVER_CLASS = "db.driver";
//
//	/** The URL to connect to the database. */
//	public static final String CONNECTION_URL = "db.url";
//
//	/** The user name to use to connect to the database. */
//	public static final String CONNECTION_USER = "db.user";
//
//	/** The password to use for establishing the connection. */
//	public static final String CONNECTION_PASSWD = "db.passwd";
//	
//	public static final boolean insertImage = false;
//	
//	public static final int totalUsers = 100000;
//	
//	public static Semaphore semaphore = new Semaphore(0);
//	public static final int numThreads = 100; 
//	
//	public static String[] clients = {
//		"10.0.1.65:10001", 
//		"10.0.1.60:10001", "10.0.1.55:10001",
//		"10.0.1.50:10001", "10.0.1.45:10001", "10.0.1.40:10001", "10.0.1.35:10001",
//	};
//	
//	public SendUpdateTest() {
//		db = new RelaxedConsistencyClient();
//		
//		Properties p = new Properties();
//		p.setProperty(DRIVER_CLASS, "oracle.jdbc.driver.OracleDriver");
//		p.setProperty(CONNECTION_URL, "jdbc:oracle:thin:@//10.0.1.80:1521/ORCL");
//		p.setProperty(CONNECTION_USER, "cosar");
//		p.setProperty(CONNECTION_PASSWD, "gocosar");
//		
//		db.setProperties(p);
//		
//		try {
//			db.init();
//		} catch (DBException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace(System.out);
//		}
//	}
//	
//	public DB getDB() {
//		return db;
//	}
//	
//	public static void main(String[] args) {
//		String mode = args[0];
//		
//		switch (mode) {
//		case "wait":
//			doWait();
//			break;
//		case "exec":
//			doExec();
//			break;
//		}
//	}
//
//	private static void doExec() {
//		new SendUpdateTest();
//		
//		Map<String, Set<String>> clients2Keys = new HashMap<>();
//		for (String c: clients) {
//			clients2Keys.put(c, new HashSet<String>());
//		}
//		
//		Random rand = new Random();
//		long start = System.currentTimeMillis();
//		for (int i = 0; i < TIME_LOOP; i++) {
//			int transId = i+1;
//			int id1 = rand.nextInt(totalUsers);
//			int id2 = rand.nextInt(totalUsers);
//			String query = CoreClient.getDML(CoreClient.INVITE, id1, id2);
//			ClientAction.sendUpdateMessages(CoreClient.clientId, transId, 
//					query, clients2Keys);
//		}
//		long total = System.currentTimeMillis() - start;
//		System.out.println(String.format("Average time sendUpdateMessages to %d is %f",
//				SendUpdateTest.clients.length, (double)total / TIME_LOOP));
//	}
//
//	private static void doWait() {
//		Vector<Thread> ts = new Vector<Thread>();
//		for (int i = 0; i < numThreads; i++) {
//			DoWarmUp wu = new DoWarmUp(i);
//			Thread t = new Thread(wu);
//			ts.add(t);
//			t.start();
//		}
//		
//		for (Thread t: ts) {
//			try {
//				t.join();
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		
//		System.out.println("Loading finished. Go sleeping now...");
//		
//		try {
//			semaphore.acquire();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//}
//
//class DoWarmUp implements Runnable {
//	SendUpdateTest test;
//	int id;
//	
//	public DoWarmUp(int id) {
//		test = new SendUpdateTest();
//		this.id = id;
//	}
//
//	@Override
//	public void run() {
//		int numUsersPerThread = SendUpdateTest.totalUsers / SendUpdateTest.numThreads;
//		System.out.println("Thread "+ id + " starts loading data... ");
//		System.out.println("Id="+id+"; Num users per thread: "+numUsersPerThread);
//		for (int i = id * numUsersPerThread; i < (id+1) * numUsersPerThread; i++) {	
//			HashMap<String, ByteIterator> result = new HashMap<>();
//			test.getDB().viewProfile(i, i, result, false, false);
//			test.getDB().viewProfile(0, i, result, false, false);
//			Vector<HashMap<String, ByteIterator>> results = new Vector<>();
//			test.getDB().viewFriendReq(i, results, false, false);
//			test.getDB().listFriends(i, i, null, results, false, false);
//		}
//	}	
//}
