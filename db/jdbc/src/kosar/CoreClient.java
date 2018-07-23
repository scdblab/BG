package kosar;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import MySQL.AggrAsAttr2R1TMySQLClient;
import kosar.AtomicBusyWaitingLock;
import kosar.AtomicCyclicInteger;
import kosar.CoreServerAddr;
import kosar.Utils;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.server.BGServer;

public class CoreClient {

	public static final char INVITE = 'I';

	public static final char ACCEPT = 'A';

	public static final char REJECT = 'R';

	public static final char THAW = 'T';

	public static final char VIEW_PROFILE = 'V';

	public static final char VIEW_PENDING = 'P';

	public static final char LIST_FRIEND = 'F';
	
	public static final char FRIEND_COUNT = 'C';
	
	public static final char PENDING_COUNT = 'Q';

	public static final String DELIMITOR = ",";

	/**
	 * the port the client socket will listen on
	 */
	public static int port = 10001;

	public static long clientId;

	/**
	 * number of semaphores that used to guard worker threads
	 */
	private int numSemaphores = 101;

	/**
	 * number of threads per semaphore
	 */
	private int numThreadsPerSemaphore = 10;

	private int initSockets = 100;

	public static final int sizeOfCacheArray = 101;

	private int sizeOfInternalKeyLockPool = 1013;

	public static final boolean enableCache = true;
	public static final boolean partitionIK = true;

	public static final boolean runWithPolicy = true;
	
	public static final boolean printCacheSize = false;

	public static final long backOffTime = 30;

	private AtomicCyclicInteger integer = new AtomicCyclicInteger(
			Integer.MAX_VALUE);

	public static final AtomicLong totalReadLatency = new AtomicLong();

	public static final AtomicLong totalWriteLatency = new AtomicLong();

	/**
	 * Client Life lease renew interval
	 */
	private long renewLeaseInterval = 10000;

	/**
	 * internal key to value cache
	 */

//	public static ConcurrentHashMap<String, Object>[] ikCache;

	private static AtomicBusyWaitingLock[] locks;

	public static final Map<Integer, Set<String>> tid2Iks = new ConcurrentHashMap<Integer, Set<String>>();

	public static final Map<Integer, Set<String>> recTid = new ConcurrentHashMap<Integer, Set<String>>();

	public static final boolean verify = false;

	public static LifeLeaseRenewer renewer;

	public static String me;	
	
	public static int READ_WORKER_SIZE = 0;
	@SuppressWarnings("unchecked")
	public static ConcurrentLinkedQueue<DoReadJob>[] readJobs = new ConcurrentLinkedQueue[READ_WORKER_SIZE];
	public static DoReadTokenWorker[] readWorkers = new DoReadTokenWorker[READ_WORKER_SIZE];
	public static Semaphore[] readSemaphores = new Semaphore[READ_WORKER_SIZE];

	/**
	 * initialize client simulator
	 * 
	 */
	@SuppressWarnings("unchecked")
	public void initialize(Properties properties) {
		CoreServerAddr.shards = properties.getProperty("cores").split(",");
		CoreServerAddr.dmlProcessAddrs = properties.getProperty("mergers").split(",");
		
//		ikCache = (ConcurrentHashMap<String, Object>[]) new ConcurrentHashMap[this.sizeOfCacheArray];
//		for (int i = 0; i < this.sizeOfCacheArray; i++) {
//			ikCache[i] = new ConcurrentHashMap<String, Object>();
//		}
//		CacheHelper.cm.setCache(ikCache);

		locks = new AtomicBusyWaitingLock[this.sizeOfInternalKeyLockPool];

		for (int i = 0; i < this.sizeOfInternalKeyLockPool; i++) {
			locks[i] = new AtomicBusyWaitingLock();
		}
		Semaphore latch = new Semaphore(0);
		new AsyncSocketServer(numSemaphores, numThreadsPerSemaphore,
				initSockets, port, latch, properties);
		try {
			latch.acquire();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		port = AsyncSocketServer.server.getLocalPort();
		System.out.println("Done Core Client listen on port " + port);
		System.out.println("register client to KOSAR Core");
		me = "";
		try {
			me = getLocalHostLANAddress().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		if (runWithPolicy) {
			if (BGServer.ClientInfo != null && BGServer.ClientInfo.size() > 0) {
				for (Integer cid : BGServer.ClientInfo.keySet()) {
					String ip = BGServer.ClientInfo.get(cid).getIP();
					if (!ip.equals(me)) {
						AsyncSocketServer.generateSocketPool(ip + ":" + port);
					}
				}
			}
		}
		clientId = Utils.genID(port);
		doActionRegister();
		renewer = new LifeLeaseRenewer(clientId, this.renewLeaseInterval);
		// Thread renewLeaseThread = new Thread(renewer);
		// renewLeaseThread.start();
		
		for (int i = 0; i < READ_WORKER_SIZE; i++) {
			readSemaphores[i] = new Semaphore(0);
			readJobs[i] = new ConcurrentLinkedQueue<>();
			
			DB db = null;
			String driver = properties.getProperty("db.driver");
			switch (driver) {
			case "mysql":
				db = new AggrAsAttr2R1TMySQLClient();
				break;
			case "oracle":
				db = new JdbcDBClient();
			default:
				break;
			}
			db.setProperties(properties);
			try {
				db.init();
			} catch (DBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			readWorkers[i] = new DoReadTokenWorker(i, db, readSemaphores[i], readJobs[i]);
		}
		
		for (int i = 0; i < READ_WORKER_SIZE; i++) {
			readWorkers[i].start();
		}
	}

	/**
	 * initialize client simulator
	 * 
	 */
	public void initializeTest(long clientId) {
		System.out.println("Core Client listen on port " + port);
		Semaphore latch = new Semaphore(0);
		new AsyncSocketServer(numSemaphores, numThreadsPerSemaphore,
				initSockets, port, latch, null);
		try {
			latch.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		CoreClient.clientId = clientId;
	}

	/**
	 * Write action for refresh technique
	 * @param dbclient
	 * @param dml
	 * @param iks
	 * @param unmarshallBuffer 
	 * @return
	 */
	public int doDMLRefresh(DB dbclient, String dml, 
			Set<String> iks, byte[] unmarshallBuffer) {
		int tid = nextTID();
		SimpleDoDMLListener doDMLListener = new SimpleDoDMLListener(dbclient, iks);
		SimpleDoReadListener doReadListener = new SimpleDoReadListener(dbclient);
		if (CoreClient.printCacheSize) {
			String action = "";
			switch (dml.charAt(0)) {
			case CoreClient.INVITE:
				action = "INVITE";
				break;
			case CoreClient.ACCEPT:
				action = "ACCEPT";
				break;
			case CoreClient.REJECT:
				action = "REJECT";
				break;
			case CoreClient.THAW:
				action = "THAW";
				break;
			}
			ClientTokenWorker.map.put(tid, action);
		}
		int ret = -1;
		
		if (ClientAction.refillMode == ClientAction.REFILL_EAGER_WRITE) {
			ret = ClientAction.doShardedDML_Refresh(tid, clientId, dml,
					doDMLListener, doReadListener, unmarshallBuffer);
		} else if (ClientAction.refillMode == ClientAction.REFILL_DELTA_READ) {
			ret = ClientAction.doShardedDML_Refill(tid, clientId, dml,
					doDMLListener, unmarshallBuffer);
		}
		
		if (CoreClient.printCacheSize) {
			String out = ClientTokenWorker.map.remove(tid);
			System.out.print(out);
		}
		return ret;
	}

	/**
	 * [ perform a write action
	 */
	public int doDML(DB db, String query, Set<String> iks) {
		int tid = nextTID();
		SimpleDoDMLListener doDMLListener = new SimpleDoDMLListener(db, iks);
		if (CoreClient.printCacheSize) {
			String action = "";
			switch (query.charAt(0)) {
			case CoreClient.INVITE:
				action = "INVITE";
				break;
			case CoreClient.ACCEPT:
				action = "ACCEPT";
				break;
			case CoreClient.REJECT:
				action = "REJECT";
				break;
			case CoreClient.THAW:
				action = "THAW";
				break;
			}
			ClientTokenWorker.map.put(tid, action);
		}
		int ret = -1;
		if (!partitionIK) {
			ret = ClientAction.doDML(clientId, tid, query,
					CoreServerAddr.getServerAddr(), doDMLListener);
		} else {
			ret = ClientAction.doShardedDML(tid, clientId, query,
					doDMLListener);
		}
		if (CoreClient.printCacheSize) {
			String out = ClientTokenWorker.map.remove(tid);
			System.out.print(out);
		}
		return ret;
	}

	/**
	 * query and ik are the same
	 */
	public Object doRead(DB db, String query, String ik, byte[] unmarshallBuffer) {
		SimpleDoReadListener doReadListener = new SimpleDoReadListener(db);
		String serverAddr;
		if (!partitionIK) {
			serverAddr = CoreServerAddr.getServerAddr();
		} else {
			serverAddr = CoreServerAddr.getServerAddr(ik);
		}
		Object value = ClientAction.doRead(clientId, serverAddr, ik, query,
				null, doReadListener, unmarshallBuffer);
		return value;

	}

	public void doActionRegister() {
		if (!partitionIK) {
			AsyncSocketServer
					.generateSocketPool(CoreServerAddr.getServerAddr());
			ClientAction.doRegister(CoreServerAddr.getServerAddr(), clientId, port);
			System.out.println("success register client " + clientId);
		} else {
			StringBuilder builder = new StringBuilder();
			for (int i = 0; i < CoreServerAddr.shards.length; i++) {
				AsyncSocketServer.generateSocketPool(CoreServerAddr.shards[i]);
				ClientAction.doRegister(CoreServerAddr.shards[i], clientId,
						port);
				builder.append(String.format("client Id %d for Core %s \n",
						clientId, CoreServerAddr.shards[i]));
			}
			System.out.println("success reigster client \n"
					+ builder.toString());
		}
	}

	public boolean doActionUnregister() {
		if (!partitionIK) {
			return ClientAction.doUnregister(clientId,
					CoreServerAddr.getServerAddr());
		} else {
			boolean success = true;
			for (String server : CoreServerAddr.shards) {
				boolean ret = ClientAction.doUnregister(clientId, server);
				if (!ret) {
					success = false;
				}
			}
			return success;
		}
	}

	public int nextTID() {
		return this.integer.incrementCycleAndGet();
	}

	public static AtomicBusyWaitingLock getLock(String key) {
		int hash = (key.hashCode() < 0 ? ((~key.hashCode()) + 1) : key
				.hashCode());
		int index = hash % locks.length;
		return locks[index];
	}

//	public static ConcurrentHashMap<String, Object> getCache(String key) {
//		int hash = (key.hashCode() < 0 ? ((~key.hashCode()) + 1) : key
//				.hashCode());
//		int index = hash % ikCache.length;
//		return ikCache[index];
//	}

	public static String getDML(char action, int inviter, int invitee) {
		StringBuilder builder = new StringBuilder();
		builder.append(action);
		builder.append(DELIMITOR);
		builder.append(inviter);
		builder.append(DELIMITOR);
		builder.append(invitee);
		return builder.toString();
	}

	public static String getQuery(char action, int requestId, int profileId,
			boolean insertImage) {
		StringBuilder builder = new StringBuilder();
		builder.append(action);
		builder.append(DELIMITOR);
		builder.append(requestId);
		builder.append(DELIMITOR);
		builder.append(profileId);
		builder.append(DELIMITOR);
		builder.append(insertImage);
		return builder.toString();
	}

	public static String getQuery(char action, int profileId,
			boolean insertImage) {
		StringBuilder builder = new StringBuilder();
		builder.append(action);
		builder.append(DELIMITOR);
		builder.append(profileId);
		builder.append(DELIMITOR);
		builder.append(insertImage);
		return builder.toString();
	}

	public static String getIK(char action, int profileId) {
		StringBuilder builder = new StringBuilder();
		builder.append(action);
		builder.append(profileId);
		return builder.toString();
	}

//	public static String getIK(char action, int requetsId, int profileId) {
//		StringBuilder builder = new StringBuilder();
//		builder.append(action);
//		builder.append('p');
//		builder.append(profileId);
//		return builder.toString();
//	}
	
	/**
	 * Returns an <code>InetAddress</code> object encapsulating what is most likely the machine's LAN IP address.
	 * <p/>
	 * This method is intended for use as a replacement of JDK method <code>InetAddress.getLocalHost</code>, because
	 * that method is ambiguous on Linux systems. Linux systems enumerate the loopback network interface the same
	 * way as regular LAN network interfaces, but the JDK <code>InetAddress.getLocalHost</code> method does not
	 * specify the algorithm used to select the address returned under such circumstances, and will often return the
	 * loopback address, which is not valid for network communication. Details
	 * <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4665037">here</a>.
	 * <p/>
	 * This method will scan all IP addresses on all network interfaces on the host machine to determine the IP address
	 * most likely to be the machine's LAN address. If the machine has multiple IP addresses, this method will prefer
	 * a site-local IP address (e.g. 192.168.x.x or 10.10.x.x, usually IPv4) if the machine has one (and will return the
	 * first site-local address if the machine has more than one), but if the machine does not hold a site-local
	 * address, this method will return simply the first non-loopback address found (IPv4 or IPv6).
	 * <p/>
	 * If this method cannot find a non-loopback address using this selection algorithm, it will fall back to
	 * calling and returning the result of JDK method <code>InetAddress.getLocalHost</code>.
	 * <p/>
	 *
	 * @throws UnknownHostException If the LAN address of the machine cannot be found.
	 */
	private static InetAddress getLocalHostLANAddress() throws UnknownHostException {
		try {
			InetAddress candidateAddress = null;
			// Iterate all NICs (network interface cards)...
			for (Enumeration<?> ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements();) {
				NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
				// Iterate all IP addresses assigned to each card...
				for (Enumeration<?> inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements();) {
					InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
					if (!inetAddr.isLoopbackAddress()) {

						if (inetAddr.isSiteLocalAddress()) {
							// Found non-loopback site-local address. Return it immediately...
							return inetAddr;
						}
						else if (candidateAddress == null) {
							// Found non-loopback address, but not necessarily site-local.
							// Store it as a candidate to be returned if site-local address is not subsequently found...
							candidateAddress = inetAddr;
							// Note that we don't repeatedly assign non-loopback non-site-local addresses as candidates,
							// only the first. For subsequent iterations, candidate will be non-null.
						}
					}
				}
			}
			if (candidateAddress != null) {
				// We did not find a site-local address, but we found some other non-loopback address.
				// Server might have a non-site-local address assigned to its NIC (or it might be running
				// IPv6 which deprecates the "site-local" concept).
				// Return this non-loopback candidate address...
				return candidateAddress;
			}
			// At this point, we did not find a non-loopback address.
			// Fall back to returning whatever InetAddress.getLocalHost() returns...
			InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
			if (jdkSuppliedAddress == null) {
				throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
			}
			return jdkSuppliedAddress;
		}
		catch (Exception e) {
			UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
			unknownHostException.initCause(e);
			throw unknownHostException;
		}
	}		
}
