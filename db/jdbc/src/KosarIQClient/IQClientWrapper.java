package KosarIQClient;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

//import KosarCoreClient.LifeLeaseRenewer;
import edu.usc.IQ.client.IQClient;
import edu.usc.IQ.client.TransactionResult;
import edu.usc.IQ.client.impl.ILeaseContext;
import edu.usc.IQ.client.impl.ILeaseContext.Result;
import edu.usc.IQ.client.impl.IQClientImpl;
import edu.usc.IQ.client.impl.IQClientMonitor;
import edu.usc.bg.base.DB;

public class IQClientWrapper {

	public static final char INVITE = 'I';

	public static final char ACCEPT = 'A';

	public static final char REJECT = 'R';

	public static final char THAW = 'T';

	public static final char VIEW_PROFILE = 'V';

	public static final char VIEW_PENDING = 'P';

	public static final char LIST_FRIEND = 'F';

	public static final String DELIMITOR = ",";

	/**
	 * the port the client socket will listen on
	 */
	public int port = 0;

	public static long clientId;

	/**
	 * number of semaphores that used to guard worker threads
	 */
	private int numSemaphores = 101;

	/**
	 * number of threads per semaphore
	 */
	private int numThreadsPerSemaphore = 10;

	private int initSockets = 1;

	private int sizeOfCacheArray = 101;

	private int sizeOfInternalKeyLockPool = 1013;

	public static final long backOffTime = 30;

	public static final AtomicLong totalReadLatency = new AtomicLong();

	public static final AtomicLong totalWriteLatency = new AtomicLong();

	/**
	 * Client Life lease renew interval
	 */
	private long renewLeaseInterval = 10000;

//	public static LifeLeaseRenewer renewer;

	public static String me;

	private static IQClient<Object> client = new IQClientMonitor<Object>();

	private IKMapper mapper;

	private CacheListenerImpl<Object> cacheListener;

	private ValueListenerImpl<Object> valueListener;

	private String[] coreAddr = new String[] { "10.0.1.75:8888","10.0.1.70:8888","10.0.1.65:8888","10.0.1.60:8888","10.0.1.55:8888","10.0.1.50:8888","10.0.1.40:8888","10.0.1.35:8888","10.0.1.25:8888","10.0.1.20:8888","10.0.1.15:8888","10.0.1.10:8888" };
//	private String[] coreAddr = new String[] { "10.0.0.240:8888"};

	public static final AtomicLong keySize = new AtomicLong(0);
	
	public static final AtomicLong payloadSize = new AtomicLong(0);
	
	/**
	 * initialize client simulator
	 * 
	 */
	public void initialize() {
		this.cacheListener = new CacheListenerImpl<Object>(
				this.sizeOfCacheArray);
		this.valueListener = new ValueListenerImpl<Object>(null);
		this.mapper = new IKMapper(this.coreAddr);
		client = new IQClientImpl<Object>();
		Set<String> addr = new HashSet<String>();
		for (String a : coreAddr) {
			addr.add(a);
		}
		client.setup(numSemaphores, numThreadsPerSemaphore,
				this.initSockets, this.port, this.sizeOfInternalKeyLockPool,
				addr, backOffTime, cacheListener, valueListener, true);
		client.register();
//		renewer = new LifeLeaseRenewer(clientId, this.renewLeaseInterval);
		// Thread renewLeaseThread = new Thread(renewer);
		// renewLeaseThread.start();
	}

	/**
	 * perform a write action
	 */
	public int doDML(DB db, String query, Set<String> iks) {
		int tid = client.getNextTransactionId();
		Set<String> cores = client.acquireQLease(tid, iks, mapper);
		int ret = new SimpleDoDMLListener(db).updateDB(query);
		TransactionResult result;
		if (ret == 0) {
			result = TransactionResult.COMMITTED;
		} else {
			result = TransactionResult.ABORTED;
		}
		client.releaseQLease(result, tid, cores);
		return ret;
	}

	/**
	 * query and ik are the same
	 */
	public Object doRead(DB db, String query, String ik, byte[] unmarshallBuffer) {
		Set<String> iks = new HashSet<String>();
		iks.add(ik);
		ValueListenerImpl<Object> listener = new ValueListenerImpl<Object>(db,
				unmarshallBuffer);
		Object value = null;
		ILeaseContext<Object> context = client.acquireILease(ik, iks, null, mapper, cacheListener, listener);
		if (Result.VALUE.equals(context.getResult())) {
			return context.getValue();
		} else if (Result.DB.equals(context.getResult())) {
			value = listener.query(query);
			context.setValue(value);
		}
		client.releaseILease(context, mapper, cacheListener);
		return context.getValue();
	}

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

	public static String getIK(char action, int requetsId, int profileId) {
		StringBuilder builder = new StringBuilder();
		builder.append(action);
		builder.append('?');
		builder.append(profileId);
		return builder.toString();
	}

	public static void shutdown() {
		System.out.println("shut down !!!");
		client.shutdown();
	}
}
