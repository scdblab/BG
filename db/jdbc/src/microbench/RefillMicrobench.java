package microbench;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import kosar.AsyncSocketServer;
import kosar.ClientBaseAction;
import kosar.CoreClient;
import kosar.CoreServerAddr;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import relational.JdbcDBClient;

public class RefillMicrobench {
	JdbcDBClient db;
	public static final String coreAddr = "10.0.0.210:8888";
	public static final int TIME_LOOP = 1000000;
	
	/** The class to use as the jdbc driver. */
	public static final String DRIVER_CLASS = "db.driver";

	/** The URL to connect to the database. */
	public static final String CONNECTION_URL = "db.url";

	/** The user name to use to connect to the database. */
	public static final String CONNECTION_USER = "db.user";

	/** The password to use for establishing the connection. */
	public static final String CONNECTION_PASSWD = "db.passwd";
	
	public static final boolean insertImage = false;
	
	CoreClient cc;
	
	public RefillMicrobench() {
		this.db = new JdbcDBClient();
		
		Properties p = new Properties();
		p.setProperty(DRIVER_CLASS, "oracle.jdbc.driver.OracleDriver");
		p.setProperty(CONNECTION_URL, "jdbc:oracle:thin:@//10.0.1.80:1521/ORCL");
		p.setProperty(CONNECTION_USER, "cosar");
		p.setProperty(CONNECTION_PASSWD, "gocosar");
		
		this.db.setProperties(p);
		
		try {
			db.init();
		} catch (DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
		
//		initSocketsToCore(p);
		cc = new CoreClient();
		CoreServerAddr.setServerAddr(new String[] {coreAddr});
		cc.initialize(p);
		
//		ClientAction.doRegister(coreAddr, 1,
//				10001);
	}
	
	public static void main(String[] args) {
		RefillMicrobench bench = new RefillMicrobench();
		
		System.out.println("Time loop: "+TIME_LOOP);
		
		bench.testAddingUserEntryListFriend();
		bench.testWriteLeases();
		
		bench.shutDown();
	}
	
	public double testAddingUserEntryListFriend() {
		HashMap<String, ByteIterator> profile = 
				new HashMap<String, ByteIterator>();
		db.viewProfile(1, 10, profile, false, false);
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < TIME_LOOP; i++) {
			HashMap<String, ByteIterator> values = 
					new HashMap<String, ByteIterator>();
			values = new HashMap<String, ByteIterator>();
			values.put("userid",
					new ObjectByteIterator((10 + "").getBytes()));
			values.put("INVITERID",
					new ObjectByteIterator((1 + "").getBytes()));
			values.put("INVITEEID",
					new ObjectByteIterator((10 + "").getBytes()));

			values.put("USERNAME", profile.get("USERNAME"));
			values.put("FNAME", profile.get("FNAME"));
			values.put("LNAME", profile.get("LNAME"));
			values.put("GENDER", profile.get("GENDER"));
			values.put("DOB", profile.get("DOB"));
			values.put("JDATE", profile.get("JDATE"));
			values.put("LDATE", profile.get("LDATE"));
			values.put("ADDRESS", profile.get("ADDRESS"));
			values.put("EMAIL", profile.get("EMAIL"));
			values.put("TEL", profile.get("TEL"));

			if (insertImage)
				values.put("tpic", profile.get("pic"));
		}
		long time = System.currentTimeMillis() - start;
		double avr = (double) time / TIME_LOOP;
		System.out.println(
				String.format("Average time constructing user entry %f",
						avr));
		return avr;
	}
	
	public double testWriteLeases() {
		Set<String> iks = new HashSet<String>();
		Set<Integer> clientIds = new HashSet<Integer>();
		
		iks.add("key1");
		iks.add("key2");
		iks.add("key3");
		iks.add("key4");
		iks.add("key5");
		iks.add("key6");
		iks.add("key7");
		
		for (String ik : iks) {
			this.fakeRead(ik);
		}	
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < TIME_LOOP; i++) {
			HashMap<String, Map<String, Long>> hm = new HashMap<>();
			Integer tid = ClientBaseAction.acquireQLease(CoreClient.clientId, coreAddr, 1, iks, hm);
			ClientBaseAction.releaseQLease(CoreClient.clientId, coreAddr, tid, iks);
		}
		long time = System.currentTimeMillis() - start;
		double avr = (double) time / TIME_LOOP;
		System.out.println("Avr write leases: "+avr);
		return avr;
	}
	
	public void fakeRead(String key) {
		Set<String> iks = new HashSet<String>();
		iks.add(key);
		Integer leaseNumber = ClientBaseAction.acquireILease(CoreClient.clientId, coreAddr, iks, key, null, null);
		ClientBaseAction.releaseILease(kosar.PacketFactory.CMD_RELEASE_I_LEASE, CoreClient.clientId, coreAddr, leaseNumber, 
				iks, key, -1, null, null);
	}
	
//	public void initSocketsToCore(Properties p) {
//		Semaphore latch = new Semaphore(0);
//		new AsyncSocketServer(101, 10,
//				100, 10001, latch, null);
//		try {
//			latch.acquire();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		AsyncSocketServer.generateSocketPool("10.0.0.210:8888");
//	}
	
	public void shutDown() {
		AsyncSocketServer.shutdown();
	}
}
