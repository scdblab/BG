package kosar;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import kosar.AtomicBusyWaitingLock;
import kosar.CoreServerAddr;
import kosar.DoDMLListener;
import kosar.DoReadListener;
import edu.usc.IQ.base.PacketFactory;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.dblab.cm.base.CacheManager;
import edu.usc.dblab.cm.sizers.StatisticalSizer;
import kosar.ClientBaseAction.ActionListener;
import kosar.DoDMLJob.JobType;

/**
 * A class that wraps all client actions that will need to interact with Core. <br/>
 * Call {@link ClientAction#doRead(int, String, String, DoReadListener)} when
 * you get a cache miss on the query. <br/>
 * Call {@link #doDML(int, int, String, DoDMLListener)} when you want to update
 * the database. <br/>
 * Call {@link #doRegister()} when client is started. <br/>
 * Call {@link #doUnregister(int)} when client want to stop using cache
 * 
 * @author Haoyu Huang
 * 
 */
public class ClientAction {

	public static final int SERVER_ID = 0;

	public static final int CMD_TX_COMMITED = 0;

	public static final int CMD_TX_ABORTED = -10;

	public static final String KEY_READ_ACTION = "READ";

	public static final String KEY_DML_ACTION = "DML";

	public static final int IQ = 0;
	public static final int NO_IQ = 1;
	public static final int I_ONLY = 2;

	public static int consistencyMode = 0;

	private static final boolean writeStep1 = true;
	private static final boolean writeStep2 = true;
	private static final boolean writeStep3 = true;

	public static final ConcurrentHashMap<String, Long> writes = new ConcurrentHashMap<String, Long>();

	// metrics related to cache hit
	public static final AtomicLong hitCache = new AtomicLong(0);

	public static final AtomicLong totalNumberOfReads = new AtomicLong(0);

	public static final AtomicLong backoffReads = new AtomicLong(0);

	public static final AtomicLong backoffWrites = new AtomicLong(0);

	public static final AtomicLong failReleasedReads = new AtomicLong(0);

	public static final AtomicLong releasedItem = new AtomicLong(0);

	public static final AtomicLong queryDB = new AtomicLong(0);

	public static final AtomicLong queryDBFromRead = new AtomicLong(0);
	
	public static final AtomicLong queryDBFromWrite = new AtomicLong(0);

	public static final AtomicLong totalNumberOfWrites = new AtomicLong(0);

	// metric related to steal policy
	public static final AtomicLong steal = new AtomicLong(0);

	public static final AtomicLong stolen = new AtomicLong(0);

	public static final AtomicLong stealNothing = new AtomicLong(0);

	public static final AtomicLong stealFromMe = new AtomicLong(0);

	public static final AtomicLong stolenFromMe = new AtomicLong(0);

	// metric related to consume policy
	public static final AtomicLong consume = new AtomicLong(0);

	public static final AtomicLong consumed = new AtomicLong(0);

	public static final AtomicLong consumeNothing = new AtomicLong(0);

	public static final AtomicLong consumeFromMe = new AtomicLong(0);

	public static final AtomicLong consumedFromMe = new AtomicLong(0);

	public static final AtomicLong getFromMyself = new AtomicLong(0);

	public static final AtomicLong updateFail = new AtomicLong(0);

	public static final AtomicLong numSendQLeases = new AtomicLong(0);

	public static final AtomicLong numberOfReadPCFriends = new AtomicLong(0);
	public static final AtomicLong avrPCFriends = new AtomicLong(0);
	public static final AtomicLong numUpdateMsgs = new AtomicLong(0);
	
	public static final AtomicLong numBackgroundReads = new AtomicLong(0);

	private static final boolean insertImage = false;

	public static final boolean ASYNC = false;

	private static final boolean sendQLeaseParallel = false;

	public static boolean log = false;
	public static StringBuffer logBuff = new StringBuffer();

	public static boolean limitFriends = true;
	public static final int MAX_FRIENDS = 10;
	public static boolean backgroundReads = false;
	
	public static final int REFILL_DELTA_READ = 1, REFILL_EAGER_WRITE = 2;
	public static int refillMode = REFILL_EAGER_WRITE;
//	public static int refillMode = REFILL_DELTA_READ;

	/**
	 * execute a read query until success
	 * 
	 * @param clientId
	 *            my client Id
	 * @param ik
	 *            internal key of the query
	 * @param query
	 *            the read query
	 * @param triggerSchema
	 *            the trigger schema associated with the query
	 * @param doReadListener
	 *            an listener on this action
	 * @return result of query
	 */
	@SuppressWarnings("unchecked")
	public static <T> T doRead(long clientId, String serverAddr, String ik,
			String query, Set<String> triggerSchemas,
			DoReadListener<T> doReadListener, byte[] unmarshallBuffer) {
		long start = System.nanoTime();
		totalNumberOfReads.incrementAndGet();

		switch (ik.charAt(0)) {
		case CoreClient.LIST_FRIEND:
		case CoreClient.VIEW_PENDING:
			numberOfReadPCFriends.incrementAndGet();
			break;
		}

		ActionListener<Map<String, Long>, List<Delta>> listener = new ActionListener<Map<String, Long>, List<Delta>>() {
			@Override
			public void onComplete() {
			}

			@Override
			public void onNetworkError(Exception e) {
			}

			@Override
			public void onFault(Throwable t) {
			}
		};

		Set<String> iks = new HashSet<String>();
		iks.add(ik);
		Object value = null;

		while (true) {
			value = CacheHelper.get(ik, unmarshallBuffer);
			if (value != null) {
				hitCache.incrementAndGet();

				switch (ik.charAt(0)) {
				case CoreClient.LIST_FRIEND:
				case CoreClient.VIEW_PENDING:
					try {
					value = getProfilesFromListProfileKeys(clientId, triggerSchemas, 
							(Vector<String>)value, doReadListener, unmarshallBuffer);
					} catch (NumberFormatException e) {
						System.out.println("Error");
						System.out.println(value.toString());
						System.exit(-1);
					}
					break;
				default:
					break;
				}

				long end = System.nanoTime();
				if (end - start > 0) {
					CoreClient.totalReadLatency.addAndGet(end - start);
				}
				return (T) value;
			}

			// try acquire I Lease
			Integer leaseNumber = ClientBaseAction.acquireILease(clientId,
					serverAddr, iks, ik, triggerSchemas, listener);

			int reponseCmdId = listener.getRepsponseCommandId();
			if (leaseNumber != null) {
				if (getFromOthers(reponseCmdId)
						&& listener.getResponseValue() != null) {
					// steal, copy, use once from others
					Map<String, Long> clients = listener.getResponseValue();
					int cmdId = listener.getRepsponseCommandId();
					int responseCMDId = -1;
					if (PacketFactory.CMD_USE_ONCE == cmdId) {
						consume.incrementAndGet();
						responseCMDId = PacketFactory.CMD_CONSUMED;
					} else if (PacketFactory.CMD_STEAL == cmdId) {
						steal.incrementAndGet();
						responseCMDId = PacketFactory.CMD_STOLEN;
					} else if (PacketFactory.CMD_COPY == cmdId) {
					}
					// pick a client and copy the value.
					long fromCid = -1;
					if (clients != null && clients.size() > 0) {
						List<String> cidList = new ArrayList<String>(
								clients.keySet());
						Collections.shuffle(cidList);
						while (cidList.size() > 0 && value == null) {
							String addr = cidList.remove(0);
							if (!addr.contains(CoreClient.me)) {
								fromCid = clients.get(addr);
								if (fromCid <= 0) {
									System.out.println("invalid user Id "
											+ fromCid);
								}
								value = ClientBaseAction.copyValue(fromCid,
										listener.getRepsponseCommandId(), addr,
										ik, unmarshallBuffer);
							} else {
								getFromMyself.incrementAndGet();
								value = CacheHelper.get(ik, unmarshallBuffer);
								if (value != null) {
									fromCid = clients.get(addr);
									hitCache.incrementAndGet();
									break;
								}
							}
						}
					}
					if (value != null) {
						if (PacketFactory.CMD_USE_ONCE == listener
								.getRepsponseCommandId()) {
							consumed.incrementAndGet();
						} else if (PacketFactory.CMD_STEAL == listener
								.getRepsponseCommandId()) {
							stolen.incrementAndGet();
						} else if (PacketFactory.CMD_COPY == listener
								.getRepsponseCommandId()) {
						}
						
						ClientBaseAction.releaseILease(
								responseCMDId, clientId, serverAddr,
								leaseNumber, iks, ik, fromCid, null, value);

						switch (ik.charAt(0)) {
						case CoreClient.LIST_FRIEND:
						case CoreClient.VIEW_PENDING:
							value = getProfilesFromListProfileKeys(clientId, triggerSchemas, 
									(Vector<String>)value, doReadListener, unmarshallBuffer);
							break;
						default:
							break;
						}
						long end = System.nanoTime();
						if (end - start > 0) {
							CoreClient.totalReadLatency.addAndGet(end - start);
						}
						return (T) value;
					} else {
						if (PacketFactory.CMD_USE_ONCE == listener
								.getRepsponseCommandId()) {
							consumeNothing.incrementAndGet();
						} else if (PacketFactory.CMD_STEAL == listener
								.getRepsponseCommandId()) {
							stealNothing.incrementAndGet();
						} else if (PacketFactory.CMD_COPY == listener
								.getRepsponseCommandId()) {
						}

						// if (listener.getResponseValue().size() > 0)
						queryDBFromRead.incrementAndGet();
						value = doReadListener.queryDB(query);
						Object val;
						switch (ik.charAt(0)) {
						case CoreClient.VIEW_PENDING:
						case CoreClient.LIST_FRIEND:
							val = ClientAction
									.computeListOfProfileKeys(
											(Vector<HashMap<String, ByteIterator>>) value);
							avrPCFriends
									.addAndGet(((Vector<HashMap<String, ByteIterator>>) value)
											.size());
							break;
						default:
							val = value;
							break;
						}

						responseCMDId = PacketFactory.CMD_RELEASE_I_LEASE;
						ClientBaseAction.releaseILease(
								responseCMDId, clientId, serverAddr,
								leaseNumber, iks, ik, -1, null, val);
						
						long end = System.nanoTime();
						if (end - start > 0) {
							CoreClient.totalReadLatency.addAndGet(end - start);
						}
						return (T) value;
					}
				} else if (getFromOthers(reponseCmdId)
						&& listener.getResponseValue() == null) {
					System.out.println("god, you didn't give me clients.");
				} else {
					List<Delta> deltas = listener.getDeltas();

					queryDBFromRead.incrementAndGet();
					value = doReadListener.queryDB(query);

					Object val;
					switch (ik.charAt(0)) {
					case CoreClient.VIEW_PENDING:
					case CoreClient.LIST_FRIEND:
						Vector<HashMap<String, ByteIterator>> res = (Vector<HashMap<String, ByteIterator>>) value;
						Vector<String> addProfileKeys = new Vector<String>();
						Vector<String> rmvProfileKeys = new Vector<String>();
						if (deltas.size() > 0) {
							for (Delta d : deltas) {
								String profileKey = (String) d.getData();
								
								switch (d.getOp()) {
								case Delta.ADD:
									if (rmvProfileKeys.contains(profileKey))
										rmvProfileKeys.remove(profileKey);
									else
										addProfileKeys.add(profileKey);
									break;
								case Delta.RMV:
									if (addProfileKeys.contains(profileKey))
										addProfileKeys.remove(profileKey);
									else
										rmvProfileKeys.add(profileKey);
									break;
								}
							}
						}
						
						for (String k : rmvProfileKeys) {
							int id = getIDFromKey(k);
							removeFriendEntry(res, id);
						}
						
						int i = res.size();
						for (String profileKey : addProfileKeys) {
							if (limitFriends && i >= MAX_FRIENDS)
								break;
							int id = getIDFromKey(ik);
							String profileQuery = constructQueryFromKey(id,
									profileKey);
							String addr = CoreServerAddr.getServerAddr(profileKey);
							HashMap<String, ByteIterator> dat = (HashMap<String, ByteIterator>) doRead(
									clientId, addr, profileKey, profileQuery,
									triggerSchemas, doReadListener,
									unmarshallBuffer);
							HashMap<String, ByteIterator> entry = computeUserEntry(dat);
							res.add(entry);
							i++;
						}
						
						val = ClientAction.computeListOfProfileKeys(res);
						avrPCFriends
								.addAndGet(((Vector<HashMap<String, ByteIterator>>) value)
										.size());
						break;
					default:
						val = value;
						applyDeltas(val, deltas);
						break;
					}

					int responseCMDId = PacketFactory.CMD_RELEASE_I_LEASE;
	
					ClientBaseAction.releaseILease(responseCMDId,
							clientId, serverAddr, leaseNumber, iks, ik, -1,
							null, val);
					
					long end = System.nanoTime();
					if (end - start > 0) {
						CoreClient.totalReadLatency.addAndGet(end - start);
					}
					return (T) value;
				}
			} else {
				backoffReads.incrementAndGet();
				if (CoreClient.backOffTime > 0) {
					try {
						Thread.sleep(CoreClient.backOffTime);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
			}
			doReadListener.incrementRetryCount();
		}
	}
	
	private static <T> Object getProfilesFromListProfileKeys(long clientId, Set<String> triggerSchemas, 
			Vector<String> profileKeys, DoReadListener<T> doReadListener, byte[] unmarshallBuffer) {					
		Vector<HashMap<String, ByteIterator>> hm = new Vector<HashMap<String, ByteIterator>>();
		int i = 0;
		for (; i < profileKeys.size(); i++) {
			if (limitFriends && i >= MAX_FRIENDS)
				break;
			String profileKey = profileKeys.get(i);
			int id = getIDFromKey(profileKey);
			String profileQuery = constructQueryFromKey(id,
					profileKey);
			String addr = CoreServerAddr.getServerAddr(profileKey);
			@SuppressWarnings("unchecked")
			HashMap<String, ByteIterator> dat = (HashMap<String, ByteIterator>) doRead(
					clientId, addr, profileKey, profileQuery,
					triggerSchemas, doReadListener,
					unmarshallBuffer);
			HashMap<String, ByteIterator> entry = computeUserEntry(dat);
			hm.add(entry);
		}
		avrPCFriends.addAndGet(i);
		return hm;
	}

	private static void removeFriendEntry(
			Vector<HashMap<String, ByteIterator>> res, int id) {
		for (HashMap<String, ByteIterator> friendEntry: res) {
			int fid = Integer.parseInt(new String(friendEntry.get("USERID").toArray()));
			if (fid == id) {
				res.remove(friendEntry);
				break;
			}
		}
	}

	private static void applyDeltas(Object val, List<Delta> deltas) {
		if (val instanceof HashMap<?, ?>) {
			@SuppressWarnings("unchecked")
			HashMap<String, ByteIterator> value = (HashMap<String, ByteIterator>) val;
			for (Delta d : deltas) {
				if (d.getOp() != Delta.INCR && d.getOp() != Delta.DECR) {
					System.out.println("Not the delta looking for");
				}

				@SuppressWarnings("unchecked")
				Pair<String, Integer> pair = (Pair<String, Integer>) d.getData();
				String k = pair.getKey();
				int x = Integer.parseInt(new String(((ObjectByteIterator) value
						.get(k)).toArray()));
				if (d.getOp() == Delta.INCR)
					x += pair.getValue();
				else if (d.getOp() == Delta.DECR)
					x -= pair.getValue();
				value.put(k, new ObjectByteIterator((x + "").getBytes()));
			}
		} else if (val instanceof Vector<?>) {
			@SuppressWarnings("unchecked")
			Vector<String> value = (Vector<String>) val;

			for (Delta d : deltas) {
				if (d.getOp() != Delta.ADD && d.getOp() != Delta.RMV) {
					System.out.println("Not the delta looking for");
				}

				value.add((String) d.getData());
			}
		}
	}

	private static String constructQueryFromKey(int requestId, String key) {
		int id = getIDFromKey(key);
		return CoreClient
				.getQuery(CoreClient.VIEW_PROFILE, id, id, insertImage);
	}

	private static int getIDFromKey(String key) {
		String str = key.replaceAll("[^-0-9]+", " ");
		List<String> list = Arrays.asList(str.trim().split(" "));
		return Integer.parseInt(list.get(0));
	}

	private static HashMap<String, ByteIterator> computeUserEntry(
			HashMap<String, ByteIterator> user1ProfileValues) {
		HashMap<String, ByteIterator> values;
		values = new HashMap<String, ByteIterator>();
		values.put("USERID", user1ProfileValues.get("USERID"));
		values.put("USERNAME", user1ProfileValues.get("USERNAME"));
		values.put("FNAME", user1ProfileValues.get("FNAME"));
		values.put("LNAME", user1ProfileValues.get("LNAME"));
		values.put("GENDER", user1ProfileValues.get("GENDER"));
		values.put("DOB", user1ProfileValues.get("DOB"));
		values.put("JDATE", user1ProfileValues.get("JDATE"));
		values.put("LDATE", user1ProfileValues.get("LDATE"));
		values.put("ADDRESS", user1ProfileValues.get("ADDRESS"));
		values.put("EMAIL", user1ProfileValues.get("EMAIL"));
		values.put("TEL", user1ProfileValues.get("TEL"));
		if (insertImage)
			values.put("tpic", user1ProfileValues.get("pic"));
		return values;
	}

	/**
	 * execute a write query
	 * 
	 * @param clientId
	 * @param transactionId
	 * @param query
	 * @param doDMLListener
	 * @return true if executed successfully, false otherwise
	 */
	public static int doDML(long clientId, int transactionId, String query,
			String serverAddr, DoDMLListener doDMLListener) {
		totalNumberOfWrites.incrementAndGet();
		long start = System.nanoTime();
		Set<String> iks = doDMLListener.getInternalKeys(query);

		ActionListener<Void, Void> listener = new ActionListener<Void, Void>() {

			@Override
			public void onComplete() {

			}

			@Override
			public void onNetworkError(Exception e) {

			}

			@Override
			public void onFault(Throwable t) {

			}
		};

		int ret = ClientBaseAction.invalidateNotification(clientId, serverAddr,
				transactionId, iks, listener);
		if (PacketFactory.CMD_Q_LEASE_REJECTED == ret) {
			while (ClientBaseAction.abortTID(clientId, serverAddr,
					transactionId, listener) == PacketFactory.CMD_REQUEST_ABORTED)
				;
			return -1;
		} else if (PacketFactory.CMD_Q_LEASE_GRANTED == ret) {

			int transactionResult = doDMLListener.updateDB(query);

//			if (CoreClient.verify) {
//				synchronized (CoreClient.ikCache) {
//					StringBuilder shouldDelete = new StringBuilder();
//					StringBuilder missed = new StringBuilder();
//					for (String ik : iks) {
//						if (CoreClient.getCache(ik).containsKey(ik)) {
//							shouldDelete.append(ik);
//							shouldDelete.append(",");
//						}
//					}
//
//					if (shouldDelete.length() > 0) {
//						Set<String> mik = CoreClient.tid2Iks.get(transactionId);
//						if (mik == null
//								&& !CoreClient.recTid
//										.containsKey(transactionId)) {
//							System.out.println("didn't receive invalidate "
//									+ transactionId);
//						} else if (mik == null
//								&& CoreClient.recTid.containsKey(transactionId)) {
//							System.out.println("I lease backoff doesn't work");
//						} else {
//							for (String mi : mik) {
//								missed.append(mi);
//								missed.append(",");
//							}
//							System.out.println(String.format(
//									"missed tid %d for iks %s", transactionId,
//									missed.toString()));
//						}
//						System.out.println(String.format(
//								"tid %d should delete %s", transactionId,
//								shouldDelete.toString()));
//					}
//				}
//			}
			if (transactionResult == 0) {
				// commit
				while (ClientBaseAction.commitTID(clientId, serverAddr,
						transactionId, listener) == PacketFactory.CMD_REQUEST_ABORTED)
					;
				long end = System.nanoTime();
				if (end - start > 0) {
					CoreClient.totalWriteLatency.addAndGet(end - start);
				}
				return 0;
			} else {
				while (ClientBaseAction.abortTID(clientId, serverAddr,
						transactionId, listener) == PacketFactory.CMD_REQUEST_ABORTED)
					;
				long end = System.nanoTime();
				if (end - start > 0) {
					CoreClient.totalWriteLatency.addAndGet(end - start);
				}
				return -1;
			}
		}
		long end = System.nanoTime();
		if (end - start > 0) {
			CoreClient.totalWriteLatency.addAndGet(end - start);
		}
		return -1;
	}

	public static int doShardedDML_Refill(int transactionId, long clientId,
			String query, DoDMLListener doDMLListener, byte[] unmarshallBuffer) {
		totalNumberOfWrites.incrementAndGet();
		long start = System.nanoTime();

		// get mapping <core, iks>
		Set<String> iks = doDMLListener.getInternalKeys(query);
		Map<String, Set<String>> shardedIKs = new HashMap<String, Set<String>>();
		for (String ik : iks) {
			String addr = CoreServerAddr.getServerAddr(ik);
			Set<String> values = shardedIKs.get(addr);
			if (values == null) {
				values = new HashSet<String>();
				shardedIKs.put(addr, values);
			}
			values.add(ik);
		}
		
		String[] tokens = query.split(",");
		int inviter = Integer.parseInt(tokens[1]);
		int invitee = Integer.parseInt(tokens[2]);
		char action = query.charAt(0);

		// compute deltas
		HashMap<String, List<Delta>> deltas = computeDeltas(clientId,
				transactionId, inviter, invitee, action);

		// mapping <key, clients> for clients that have copied values of the key.
		Map<String, Map<String, Long>> keys2Clients = new HashMap<String, Map<String, Long>>();
		int txResult = 0;
		while (true) {
			int res = sendAcquireQLease2(clientId, transactionId, shardedIKs,
					keys2Clients, deltas);
			if (res == PacketFactory.CMD_Q_LEASE_REJECTED) {
				System.out
						.println("sendAcquireQLease return CMD_Q_LEASE_REJECTED");
				keys2Clients.clear();
				try {
					Thread.sleep(CoreClient.backOffTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
				continue;
			}

			Map<String, Set<String>> clients2Keys = getClient2Keys(keys2Clients);
			Set<String> keys = keys2Clients.keySet();
			
			HashMap<String, Object> data = new HashMap<String, Object>();
			getCache(true, inviter, invitee, action, iks, keys2Clients, null, unmarshallBuffer, data);
			updateCache(inviter, invitee, action, data, clientId, transactionId);

			// update remote copies
			sendUpdateMessages(clientId, transactionId, 
					query, clients2Keys, keys);
			
			boolean priority = keys2Clients.size() != iks.size() ? true : false;

			HashMap<String, Set<String>> newShardedIKs = new HashMap<String, Set<String>>();
			Set<String> newIks = new HashSet<String>();
			for (String addr: shardedIKs.keySet()) {
				for (String ik :shardedIKs.get(addr)) {
					if (keys.contains(ik)) {
						Set<String> ikSet = newShardedIKs.get(addr);
						if (ikSet == null) {
							ikSet = new HashSet<String>();
							newShardedIKs.put(addr, ikSet);
						}
						ikSet.add(ik);
					} else {
						newIks.add(ik);
					}
				}
			}
			
			sendDML(clientId, inviter, invitee, query, newIks, transactionId,
					priority);
			
			if (newShardedIKs.size() != shardedIKs.size()) {
				System.out.println("iks different size");
			}
			
			if (newShardedIKs.size() > 0) 
				res = sendReleaseQLease(clientId, transactionId, newShardedIKs);

			break;
		}

		long end = System.nanoTime();
		if (end - start > 0) {
			CoreClient.totalWriteLatency.addAndGet(end - start);
		}

		return txResult;
	}

	private static int sendAcquireQLease2(long clientId, int transactionId,
			Map<String, Set<String>> shardedIKs,
			Map<String, Map<String, Long>> keys2Clients,
			HashMap<String, List<Delta>> deltas) {
		Set<String> serverIds = shardedIKs.keySet();
		int res = PacketFactory.CMD_Q_LEASE_GRANTED;
		List<String> servers = new ArrayList<String>(serverIds);
		Collections.sort(servers);
		for (String serverId : servers) {
			HashMap<String, Map<String, Long>> hm = new HashMap<String, Map<String, Long>>();
			HashMap<String, List<Delta>> ik2Deltas = new HashMap<String, List<Delta>>();
			for (String ik : shardedIKs.get(serverId)) {
				ik2Deltas.put(ik, deltas.get(ik));
			}
			int r = ClientBaseAction.acquireQLease2(clientId, serverId,
					transactionId, ik2Deltas, hm);
			if (r == PacketFactory.CMD_Q_LEASE_REJECTED) {
				res = PacketFactory.CMD_Q_LEASE_REJECTED;
				break;
			} else {
				for (String key : hm.keySet()) {
					Map<String, Long> clients = keys2Clients.get(key);
					if (clients == null) {
						clients = new HashMap<String, Long>();
						keys2Clients.put(key, clients);
					}
					clients.putAll(hm.get(key));
				}
			}
		}

		return res;
	}

	private static HashMap<String, List<Delta>> computeDeltas(long clientId,
			int transactionId, int inviter, int invitee, char action) {
		Delta d;

		HashMap<String, List<Delta>> hm = new HashMap<String, List<Delta>>();
		List<Delta> dl;

		switch (action) {
		case CoreClient.INVITE:
			d = new Delta(clientId, transactionId, Delta.INCR,
					new Pair<String, Integer>("pendingcount", 1));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee), dl);

			d = new Delta(clientId, transactionId, Delta.ADD, CoreClient.getIK(
					CoreClient.VIEW_PROFILE, inviter));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.VIEW_PENDING, invitee), dl);
			break;
		case CoreClient.REJECT:
			d = new Delta(clientId, transactionId, Delta.DECR,
					new Pair<String, Integer>("pendingcount", 1));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee), dl);

			d = new Delta(clientId, transactionId, Delta.RMV, CoreClient.getIK(
					CoreClient.VIEW_PROFILE, inviter));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.VIEW_PENDING, invitee), dl);
			break;
		case CoreClient.ACCEPT:
			d = new Delta(clientId, transactionId, Delta.DECR,
					new Pair<String, Integer>("pendingcount", 1));
			dl = new LinkedList<Delta>();
			dl.add(d);
			d = new Delta(clientId, transactionId, Delta.INCR,
					new Pair<String, Integer>("friendcount", 1));
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee), dl);

			d = new Delta(clientId, transactionId, Delta.INCR,
					new Pair<String, Integer>("friendcount", 1));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter), dl);

			d = new Delta(clientId, transactionId, Delta.RMV, CoreClient.getIK(
					CoreClient.VIEW_PROFILE, inviter));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.VIEW_PENDING, invitee), dl);

			d = new Delta(clientId, transactionId, Delta.ADD, CoreClient.getIK(
					CoreClient.VIEW_PROFILE, inviter));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.LIST_FRIEND, invitee), dl);

			d = new Delta(clientId, transactionId, Delta.ADD, CoreClient.getIK(
					CoreClient.VIEW_PROFILE, invitee));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.LIST_FRIEND, inviter), dl);
			break;
		case CoreClient.THAW:
			d = new Delta(clientId, transactionId, Delta.DECR,
					new Pair<String, Integer>("friendcount", 1));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee), dl);

			d = new Delta(clientId, transactionId, Delta.DECR,
					new Pair<String, Integer>("friendcount", 1));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter), dl);

			d = new Delta(clientId, transactionId, Delta.RMV, CoreClient.getIK(
					CoreClient.VIEW_PROFILE, inviter));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.LIST_FRIEND, invitee), dl);

			d = new Delta(clientId, transactionId, Delta.RMV, CoreClient.getIK(
					CoreClient.VIEW_PROFILE, invitee));
			dl = new LinkedList<Delta>();
			dl.add(d);
			hm.put(CoreClient.getIK(CoreClient.LIST_FRIEND, inviter), dl);
			break;
		}

		return hm;
	}

	public static int doShardedDML_Refresh(int transactionId, long clientId,
			String query, DoDMLListener doDMLListener,
			DoReadListener<?> doReadListener, byte[] unmarshallBuffer) {
		totalNumberOfWrites.incrementAndGet();
		long start = System.nanoTime();

		// get mapping <core, internal tokens>
		Set<String> iks = doDMLListener.getInternalKeys(query);
		Map<String, Set<String>> shardedIKs = new HashMap<String, Set<String>>();
		for (String ik : iks) {
			String addr = CoreServerAddr.getServerAddr(ik);
			Set<String> values = shardedIKs.get(addr);
			if (values == null) {
				values = new HashSet<String>();
				shardedIKs.put(addr, values);
			}
			values.add(ik);
		}
		
		String[] tokens = query.split(",");
		int inviter = Integer.parseInt(tokens[1]);
		int invitee = Integer.parseInt(tokens[2]);
		char action = query.charAt(0);

		Map<String, Map<String, Long>> keys2Clients = new HashMap<String, Map<String, Long>>();
		int txResult = 0;
		while (true) {
			int res = sendAcquireQLease(clientId, transactionId, shardedIKs,
					keys2Clients);
			if (res == PacketFactory.CMD_Q_LEASE_REJECTED) {
				System.out
						.println("sendAcquireQLease return CMD_Q_LEASE_REJECTED");
				keys2Clients.clear();
				try {
					Thread.sleep(CoreClient.backOffTime);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
				continue;
			}

			if (log)
				logBuff.append(query + "\n");

			// get all key-value pairs
			HashMap<String, Object> data;
			if (writeStep2) {
				data = new HashMap<String, Object>();
				getCache(false, inviter, invitee, action, iks, keys2Clients, doReadListener,
						unmarshallBuffer, data);
			}

			Map<String, Set<String>> clients2Keys = getClient2Keys(keys2Clients);
			Set<String> keys = keys2Clients.keySet();

			if (writeStep1) {
				if (!ASYNC) {
					txResult = doDMLListener.updateDB(query);
				} else {
					sendDML(clientId, inviter, invitee, query, null, transactionId, false);
					txResult = 0; // assume the DML execution always succeeds
				}
			}

			// update cache
			if (writeStep2)
				updateCache(inviter, invitee, action, data, clientId, transactionId);

			if (writeStep3) {
				sendUpdateMessages(clientId, transactionId, query,
						clients2Keys, keys);
			}

			res = sendReleaseQLease(clientId, transactionId, shardedIKs);
			break;
		}

		long end = System.nanoTime();
		if (end - start > 0) {
			CoreClient.totalWriteLatency.addAndGet(end - start);
		}

		return txResult;
	}

	protected static void updateCache(int inviter, int invitee, char action,
			HashMap<String, Object> data, long clientId, int transactionId) {
		Object val;
		String key;
		String profileKey; 
		Delta d;
		HashMap<String, Integer> m;
		switch (action) {
		case CoreClient.INVITE:
			key = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee);
			val = data.get(key);
			m = new HashMap<String, Integer>();
			m.put("pendingcount", 1);
			d = new Delta(clientId, transactionId, Delta.INCR, m);
			CacheHelper.updateCache(val, d);

			key = CoreClient.getIK(CoreClient.VIEW_PENDING, invitee);
			val = data.get(key);
			profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter);
			d = new Delta(clientId, transactionId, Delta.ADD, profileKey);
			CacheHelper.updateCache(val, d);
			break;
		case CoreClient.REJECT:
			key = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee);
			val = data.get(key);
			m = new HashMap<String, Integer>();
			m.put("pendingcount", 1);
			d = new Delta(clientId, transactionId, Delta.DECR, m);
			CacheHelper.updateCache(val, d);

			key = CoreClient.getIK(CoreClient.VIEW_PENDING, invitee);
			val = data.get(key);
			profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter);
			d = new Delta(clientId, transactionId, Delta.RMV, profileKey);
			CacheHelper.updateCache(val, d);
			break;
		case CoreClient.ACCEPT:
			key = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee);
			val = data.get(key);
			m = new HashMap<String, Integer>();
			m.put("pendingcount", 1);
			d = new Delta(clientId, transactionId, Delta.DECR, m);
			CacheHelper.updateCache(val, d);
			m = new HashMap<String, Integer>();
			m.put("friendcount", 1);
			d = new Delta(clientId, transactionId, Delta.INCR, m);
			CacheHelper.updateCache(val, d);

			key = CoreClient.getIK(CoreClient.VIEW_PENDING, invitee);
			val = data.get(key);
			profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter);
			d = new Delta(clientId, transactionId, Delta.RMV, profileKey);
			CacheHelper.updateCache(val, d);

			key = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter);
			val = data.get(key);
			m = new HashMap<String, Integer>();
			m.put("friendcount", 1);
			d = new Delta(clientId, transactionId, Delta.INCR, m);
			CacheHelper.updateCache(val, d);

			key = CoreClient.getIK(CoreClient.LIST_FRIEND, invitee);
			val = data.get(key);
			profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter);
			d = new Delta(clientId, transactionId, Delta.ADD, profileKey);
			CacheHelper.updateCache(val, d);

			key = CoreClient.getIK(CoreClient.LIST_FRIEND, inviter);
			val = data.get(key);
			profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee);
			d = new Delta(clientId, transactionId, Delta.ADD, profileKey);
			CacheHelper.updateCache(val, d);
			break;
		case CoreClient.THAW:
			key = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter);
			val = data.get(key);
			m = new HashMap<String, Integer>();
			m.put("friendcount", 1);
			d = new Delta(clientId, transactionId, Delta.DECR, m);
			CacheHelper.updateCache(val, d);

			key = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee);
			val = data.get(key);
			m = new HashMap<String, Integer>();
			m.put("friendcount", 1);
			d = new Delta(clientId, transactionId, Delta.DECR, m);
			CacheHelper.updateCache(val, d);

			key = CoreClient.getIK(CoreClient.LIST_FRIEND, invitee);
			val = data.get(key);
			profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter);
			d = new Delta(clientId, transactionId, Delta.RMV, profileKey);
			CacheHelper.updateCache(val, d);

			key = CoreClient.getIK(CoreClient.LIST_FRIEND, inviter);
			val = data.get(key);
			profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee);
			d = new Delta(clientId, transactionId, Delta.RMV, profileKey);
			CacheHelper.updateCache(val, d);
			break;
		}

		for (String k : data.keySet()) {
			CacheHelper.set(k, data.get(k));
//			CoreClient.getCache(k).put(k, data.get(k));
		}
	}

	protected static void getCache(boolean getLocalOnly, int inviter, int invitee, char action,
			Set<String> iks, Map<String, Map<String, Long>> keys2Clients,
			DoReadListener<?> doReadListener, byte[] unmarshallBuffer,
			HashMap<String, Object> data) {

		for (String ik : iks) {
			Object val = getLocalCache(ik, unmarshallBuffer);
			if (val == null) {
				if (getLocalOnly) {
					if (backgroundReads) {
						int idx = ik.hashCode();
						idx = idx > 0 ? idx : -idx;
						idx = idx % CoreClient.READ_WORKER_SIZE;
						CoreClient.readJobs[idx].add(new DoReadJob(ik));
						CoreClient.readSemaphores[idx].release();
						numBackgroundReads.incrementAndGet();
					}
					continue;
				}

				val = getRemoteCacheRefresh(ik, keys2Clients.get(ik),
						unmarshallBuffer);
				if (val == null) {
					String q = getQuery(ik, inviter, invitee);
					val = getDBMSRefresh(q, ik, doReadListener);
				}

				// value copied from others must also include deltas
				if (val != null && CacheHelper.usingDeltaCommit) {
					val = new Value(val);
				}
			}

			if (val == null) {
				System.out.println("Error: cannot get data getCache");
			} else {
				data.put(ik, val);
			}
		}
	}

	private static String getQuery(String ik, int inviter, int invitee) {
		int id = Integer.parseInt(ik.replaceAll("[^0-9]+", ""));

		char c = ik.charAt(0);
		switch (c) {
		case CoreClient.VIEW_PROFILE:
			if (!ik.contains("?")) {
				return CoreClient.getQuery(c, id, id, insertImage);
			} else {
				if (id == inviter) {
					return CoreClient
							.getQuery(c, inviter, inviter, insertImage);
				} else {
					return CoreClient
							.getQuery(c, invitee, invitee, insertImage);
				}
			}
		case CoreClient.VIEW_PENDING:
			if (id != invitee) {
				System.out.println("Error: id is different than invitee.");
				return null;
			}
			return CoreClient.getQuery(c, invitee, insertImage);
		case CoreClient.LIST_FRIEND:
			if (id == inviter) {
				return CoreClient.getQuery(c, invitee, inviter, insertImage);
			} else {
				return CoreClient.getQuery(c, inviter, invitee, insertImage);
			}
		case CoreClient.INVITE:
		case CoreClient.ACCEPT:
		case CoreClient.REJECT:
		case CoreClient.THAW:
			System.out.println("Error: not supported.");
			return null;
		default:
			return null;
		}
	}

	private static void sendDML(long clientid, int inviter, 
			int invitee, String query, Set<String> iks,
			int transactionId, boolean priority) {
		String serverAddr = CoreServerAddr.getDMLProcessAddr(inviter, invitee);
		ClientBaseAction.sendDML(clientid, serverAddr, transactionId, query,
				iks, priority);
	}

	public static String getDMLFromQuery(char action, int inviter, int invitee) {
		switch (action) {
		case CoreClient.INVITE:
			return String.format("INSERT INTO friendship values(%d,%d,1)",
					inviter, invitee);
		case CoreClient.REJECT:
			return String
					.format("DELETE FROM friendship WHERE inviterid=%d and inviteeid=%d and status=1",
							inviter, invitee);
		case CoreClient.ACCEPT:
			return String
					.format("UPDATE friendship SET status = 2 WHERE inviterid=%d and inviteeid= %d",
							inviter, invitee);
		case CoreClient.THAW:
			return String
					.format("DELETE FROM friendship WHERE (inviterid=%d and inviteeid= %d) OR (inviterid=%d and inviteeid= %d) and status=2",
							inviter, invitee, invitee, inviter);
		default:
			return null;
		}
	}

	// private static void updateCache(String query,
	// Map<String, Map<String, Long>> keys2Clients,
	// DoReadListener doReadListener,
	// byte[] unmarshallBuffer) {
	// String[] ems = query.split(",");
	// int inviter = Integer.parseInt(ems[1]);
	// int invitee = Integer.parseInt(ems[2]);
	//
	// boolean updateRemote = true;
	//
	// switch (query.charAt(0)) {
	// case CoreClient.INVITE:
	// updateProfilePending(invitee, 1, keys2Clients, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updatePendingFriendsInvite(inviter, invitee, keys2Clients,
	// doReadListener, unmarshallBuffer, updateRemote);
	// break;
	// case CoreClient.REJECT:
	// updateProfilePending(invitee, -1, keys2Clients, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updatePendingFriendsRemove(inviter, invitee, keys2Clients,
	// doReadListener, unmarshallBuffer, updateRemote);
	// break;
	// case CoreClient.ACCEPT:
	// updateProfilePending(invitee, -1, keys2Clients, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updatePendingFriendsRemove(inviter, invitee, keys2Clients,
	// doReadListener, unmarshallBuffer, updateRemote);
	// updateProfile(inviter, 1, keys2Clients, invitee, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updateProfile(inviter, 1, keys2Clients, inviter, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updateProfile(invitee, 1, keys2Clients, inviter, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updateProfile(invitee, 1, keys2Clients, invitee, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updateListFriendsAccepted(inviter, invitee, keys2Clients,
	// doReadListener, unmarshallBuffer, updateRemote);
	// updateListFriendsAccepted(invitee, inviter, keys2Clients,
	// doReadListener, unmarshallBuffer, updateRemote);
	// break;
	// case CoreClient.THAW:
	// updateProfile(inviter, -1, keys2Clients, invitee, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updateProfile(inviter, -1, keys2Clients, inviter, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updateProfile(invitee, -1, keys2Clients, inviter, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updateProfile(invitee, -1, keys2Clients, invitee, doReadListener,
	// unmarshallBuffer, updateRemote);
	// updateListFriendsRemove(inviter, invitee, keys2Clients,
	// doReadListener, unmarshallBuffer, updateRemote);
	// updateListFriendsRemove(invitee, inviter, keys2Clients,
	// doReadListener, unmarshallBuffer, updateRemote);
	// break;
	// }
	// }

	public static void sendUpdateMessages(long clientId, int transactionId,
			String query, Map<String, Set<String>> clients2Keys,
			Set<String> keys) {
		for (String client : clients2Keys.keySet()) {
			if (client.contains(CoreClient.me)) {
				clients2Keys.remove(client);
				break;
			}
		}

		if (clients2Keys.size() > 0) {
			AtomicInteger jobCounts = new AtomicInteger(clients2Keys.size());
			Semaphore latch = new Semaphore(0);
			for (String client : clients2Keys.keySet()) {
				DoDMLJob job = new DoDMLJob(JobType.REQUEST_UPDATE, clientId,
						transactionId,
						// client, query, clients2Keys.get(client), null, latch,
						// jobCounts);
						client, query, keys, null, latch, jobCounts);
				AsyncSocketServer.submitJob(job);
			}

			try {
				latch.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			numUpdateMsgs.addAndGet(clients2Keys.size());
		}
	}

	private static Map<String, Set<String>> getClient2Keys(
			Map<String, Map<String, Long>> keys2Clients) {
		Map<String, Set<String>> clients2Keys = new HashMap<String, Set<String>>();

		// Set<String> clis = new HashSet<String>();
		// clis.add("10.0.1.70:10001");
		// clis.add("10.0.1.65:10001");
		// clis.add("10.0.1.60:10001");
		// clis.add("10.0.1.55:10001");
		// for (String cli: clis) {
		// Set<String> set = new HashSet<String>();
		// set.addAll(keys2Clients.keySet());
		// clients2Keys.put(cli, set);
		// }

		for (String key : keys2Clients.keySet()) {
			Map<String, Long> clients = keys2Clients.get(key);
			for (String client : clients.keySet()) {
				Set<String> set = clients2Keys.get(client);
				if (set == null) {
					set = new HashSet<String>();
					// set.addAll(keys2Clients.keySet());
					clients2Keys.put(client, set);
				}
				set.add(key);
			}
		}

		// if (clients2Keys.keySet().size() < 4) {
		// System.out.println(clients2Keys.keySet());
		// }

		return clients2Keys;
	}

	private static int sendAcquireQLease(long clientId, int transactionId,
			Map<String, Set<String>> shardedIKs,
			Map<String, Map<String, Long>> keys2Clients) {
		Set<String> serverIds = shardedIKs.keySet();
		Set<DoDMLJob> rejectJobs = new HashSet<DoDMLJob>();

		int res = PacketFactory.CMD_Q_LEASE_GRANTED;
		if (sendQLeaseParallel) {
			Semaphore latch = new Semaphore(0);
			AtomicInteger count = new AtomicInteger(shardedIKs.size());
			ArrayList<DoDMLJob> jobs = new ArrayList<DoDMLJob>();

			for (String serverId : serverIds) {
				DoDMLJob job = new DoDMLJob(JobType.ACQUIRE_Q_LEASE_REFRESH,
						clientId, serverId, transactionId,
						shardedIKs.get(serverId), -1, latch, count);
				AsyncSocketServer.submitJob(job);
				jobs.add(job);
			}

			numSendQLeases.addAndGet(serverIds.size());

			try {
				latch.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			for (DoDMLJob job : jobs) {
				if (job.getResult() == PacketFactory.CMD_Q_LEASE_REJECTED) {
					res = PacketFactory.CMD_Q_LEASE_REJECTED;
					continue;
				}

				if (job.getResult() == PacketFactory.CMD_Q_LEASE_GRANTED) {
					rejectJobs.add(job);

					Map<String, Map<String, Long>> hm = job.getKeys2Clients();
					for (String key : hm.keySet()) {
						Map<String, Long> clients = keys2Clients.get(key);
						if (clients == null) {
							clients = new HashMap<String, Long>();
							keys2Clients.put(key, clients);
						}
						clients.putAll(hm.get(key));
					}
				}
			}
		} else {
			List<String> servers = new ArrayList<String>(serverIds);
			Collections.sort(servers);
			for (String serverId : servers) {
				HashMap<String, Map<String, Long>> hm = new HashMap<String, Map<String, Long>>();
				int r = ClientBaseAction.acquireQLease(clientId, serverId,
						transactionId, shardedIKs.get(serverId), hm);
				if (r == PacketFactory.CMD_Q_LEASE_REJECTED) {
					res = PacketFactory.CMD_Q_LEASE_REJECTED;
					break;
				} else {
					DoDMLJob job = new DoDMLJob(JobType.ABORT_Q_LEASE_REFRESH,
							clientId, serverId, transactionId,
							shardedIKs.get(serverId), -1, null, null);
					rejectJobs.add(job);

					for (String key : hm.keySet()) {
						Map<String, Long> clients = keys2Clients.get(key);
						if (clients == null) {
							clients = new HashMap<String, Long>();
							keys2Clients.put(key, clients);
						}
						clients.putAll(hm.get(key));
					}
				}
			}
		}

		// reject Q leases asynchronously if fails
		if (res == PacketFactory.CMD_Q_LEASE_REJECTED) {
			if (!sendQLeaseParallel) {
				System.out.println("This should never been reached.");
			}
			backoffWrites.incrementAndGet();
			for (DoDMLJob job : rejectJobs) {
				job.setJobType(JobType.ABORT_Q_LEASE_REFRESH);
				job.setLatch(null);
				job.setJobCounts(null);
				AsyncSocketServer.submitJob(job);
			}
			numSendQLeases.addAndGet(rejectJobs.size());
		}

		return res;
	}

	private static int sendReleaseQLease(long clientId, int transactionId,
			Map<String, Set<String>> shardedIKs) {
		Semaphore latch = new Semaphore(0);
		AtomicInteger count = new AtomicInteger(shardedIKs.size());
		Set<String> serverIds = shardedIKs.keySet();
		ArrayList<DoDMLJob> jobs = new ArrayList<DoDMLJob>();
		for (String serverId : serverIds) {
			DoDMLJob job = new DoDMLJob(JobType.RELEASE_Q_LEASE_REFRESH,
					clientId, serverId, transactionId,
					shardedIKs.get(serverId), -1, latch, count);
			AsyncSocketServer.submitJob(job);
			jobs.add(job);
		}

		try {
			latch.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		numSendQLeases.addAndGet(serverIds.size());

		int res = PacketFactory.CMD_Q_LEASE_RELEASED;
		for (DoDMLJob job : jobs) {
			if (job.getResult() == PacketFactory.CMD_Q_LEASE_ABORTED) {
				res = PacketFactory.CMD_Q_LEASE_ABORTED;
				break;
			}
		}

		return res;
	}

	public static int doShardedDML(int transactionId, long clientId,
			String query, DoDMLListener doDMLListener) {
		totalNumberOfWrites.incrementAndGet();
		long start = System.nanoTime();
		Set<String> iks = doDMLListener.getInternalKeys(query);
		Map<String, Set<String>> shardedIKs = new HashMap<String, Set<String>>();
		for (String ik : iks) {
			String addr = CoreServerAddr.getServerAddr(ik);
			Set<String> values = shardedIKs.get(addr);
			if (values == null) {
				values = new HashSet<String>();
				shardedIKs.put(addr, values);
			}
			values.add(ik);
		}

		Semaphore latch = new Semaphore(0);
		AtomicInteger count = new AtomicInteger(shardedIKs.size());
		Set<String> serverIds = shardedIKs.keySet();
		for (String serverId : serverIds) {
			DoDMLJob job = new DoDMLJob(JobType.INVALIDATE, clientId, serverId,
					transactionId, shardedIKs.get(serverId), -1, latch, count);
			AsyncSocketServer.submitJob(job);
		}
		try {
			latch.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		int txResult = doDMLListener.updateDB(query);

		latch = new Semaphore(0);
		count = new AtomicInteger(shardedIKs.size());
		for (String serverId : serverIds) {
			DoDMLJob job = new DoDMLJob(JobType.RELEASE, clientId, serverId,
					transactionId, shardedIKs.get(serverId), txResult, latch,
					count);
			AsyncSocketServer.submitJob(job);
		}
		try {
			latch.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long end = System.nanoTime();
		if (end - start > 0) {
			CoreClient.totalWriteLatency.addAndGet(end - start);
		}
		return txResult;
	}

	/**
	 * register client itself to Core to obtain a client Id
	 * 
	 * @return client Id or -1 if register failed
	 */
	public static long doRegister(String serverAddr, long clientId,
			int clientListeningPort) {
		return ClientBaseAction.register(serverAddr, clientId,
				clientListeningPort, null);
	}

	/**
	 * unregister client to Core
	 * 
	 * @param clientId
	 * @return true if unregister successfully
	 */
	public static boolean doUnregister(long clientId, String serverAddr) {
		return ClientBaseAction.unregister(clientId, serverAddr, null);
	}

	private static final boolean getFromOthers(int cmdId) {
		return (cmdId == PacketFactory.CMD_STEAL)
				|| (cmdId == PacketFactory.CMD_COPY)
				|| (cmdId == PacketFactory.CMD_USE_ONCE);
	}

	public static void log() {
		String ip = "";
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		File file = new File("/home/hieun/Desktop/ratinglogs/cl-metric-" + ip
				+ ".txt");
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			StringBuilder builder = new StringBuilder();
			builder.append(String.format(
					"writes %d,  reads %d hit %d ratio %f \n",
					totalNumberOfWrites.longValue(),
					totalNumberOfReads.longValue(),
					hitCache.longValue(),
					totalNumberOfReads.longValue() == 0 ? 0 : (double) hitCache
							.longValue()
							/ (double) totalNumberOfReads.longValue()));
			double backoffRatio = (double) backoffReads.longValue()
					/ (double) totalNumberOfReads.longValue();
			double failRatio = (double) failReleasedReads.longValue()
					/ (double) totalNumberOfReads.longValue();
			builder.append(String.format("query db %d \n", queryDB.get()));
			builder.append(String.format("num ViewProfile query %d \n",
					SimpleDoReadListener.numViewProfileQueries.get()));
			builder.append(String.format("query ListFriends %d \n",
					SimpleDoReadListener.numListFriendsQueries.get()));
			builder.append(String.format("query PendingFriends %d \n",
					SimpleDoReadListener.numViewPendingQueries.get()));
			builder.append(String.format("query FriendCount %d \n",
					SimpleDoReadListener.numFriendCountQueries.get()));
			builder.append(String.format("query PendingCount %d \n",
					SimpleDoReadListener.numPendingCountQueries.get()));

			builder.append(String
					.format("backoff reads %d failRelease reads %d backoff ratio %f fail ratio %f \n",
							backoffReads.longValue(),
							failReleasedReads.longValue(), backoffRatio,
							failRatio));
			builder.append(String.format("backoff Q lease acquire %d \n",
					backoffWrites.longValue()));
			builder.append(String.format("update fail %d \n",
					updateFail.longValue()));
			builder.append("STEAL\n");
			builder.append(String
					.format("steal %d, stolen %d, steal from me %d, stolen from me %d, get from myself %d \n",
							steal.get(), stolen.get(), stealFromMe.get(),
							stolenFromMe.get(), getFromMyself.get()));
			builder.append("CONSUME\n");
			builder.append(String
					.format("consume %d, consumed %d, consume from me %d, "
							+ "consumed from me %d, get from myself %d, consume nothing %d \n",
							consume.get(), consumed.get(), consumeFromMe.get(),
							consumedFromMe.get(), getFromMyself.get(),
							consumeNothing.get()));
			builder.append(String.format("Average sendQLease per writes: %f\n",
					(double) numSendQLeases.get() / totalNumberOfWrites.get()));
			builder.append(String.format(
					"avr pc friends length on reads: %f\n",
					(double) avrPCFriends.get() / numberOfReadPCFriends.get()));
			builder.append(String.format("Total num sendUpdateMessages: %d\n",
					numUpdateMsgs.get()));
			builder.append(String.format("Num background reads: %d\n",
					numBackgroundReads.get()));
			
			if (CacheHelper.cm != null) {
				builder.append(String.format("Cache Evict: %d\n",
						CacheHelper.cm.cacheEvict.get()));
				builder.append(String.format("Evict Nothing: %d\n",
						CacheHelper.cm.evictNothing.get()));
				builder.append(String.format("Cache Items Serialized (by background thread): %d\n",
						CacheHelper.cm.cacheItemSerialized.get()));
				builder.append(String.format("Cache Item Deserialized (by ground thread): %d\n",
						CacheHelper.cm.cacheItemDeserialized.get()));
				if (CacheHelper.cm.getSizeInspectionMethod() == CacheManager.STATISTIC) {
					StatisticalSizer statsSizer = (StatisticalSizer) CacheHelper.cm.getSizer();
					builder.append(String.format("NumGetSize: %d\n",
						statsSizer.numGetSize.get()));
					builder.append(String.format("NumProfiler: %d\n",
						statsSizer.numProfiler.get()));
					builder.append(String.format("NumReset: %d\n",
						statsSizer.numReset.get()));
				}
			}
			// builder.append(String.format(
			// "consume key size %s payload size %s \n",
			// keySize.toString(), payloadSize.toString()));
			// if (consume.get() > 0) {
			// builder.append(String.format(
			// "consume average key size %s payload size %s \n",
			// keySize.divide(BigInteger.valueOf(consume.get()))
			// .toString(),
			// payloadSize.divide(BigInteger.valueOf(consume.get()))
			// .toString()));
			// }
			builder.append("LATENCY\n");
			builder.append(String.format("average read latency %f \n",
					totalNumberOfReads.longValue() == 0 ? 0
							: (double) CoreClient.totalReadLatency.get()
									/ (double) totalNumberOfReads.get()));
			builder.append(String.format("average write latency %f \n",
					totalNumberOfWrites.get() == 0 ? 0
							: (double) CoreClient.totalWriteLatency.get()
									/ (double) totalNumberOfWrites.get()));
			builder.append(String.format(
					"total keys this client invalidates: %d",
					ClientTokenWorker.invalidatedItem.get()));
			builder.append(String.format("total query dbms in reads: %d",
					queryDBFromRead.get()));
			builder.append(String.format("total query dbms in writes: %d",
					queryDBFromWrite.get()));
			if (CoreClient.verify) {
				int cacheSize = CacheHelper.cm.count();
				builder.append(String.format("released %d deleted %d exist %d",
						releasedItem.get(),
						ClientTokenWorker.invalidatedItem.get(), cacheSize));
				builder.append(String.format("match %d", releasedItem.get()
						- ClientTokenWorker.invalidatedItem.get() - cacheSize));
			}
			System.out.println(builder.toString());
			bw.write(builder.toString());
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (log) {
			System.out.println("Log writes");

			file = new File("log-" + ip);
			try {
				if (!file.exists()) {
					file.createNewFile();
				}
				BufferedWriter bw = new BufferedWriter(new FileWriter(file));
				bw.write(logBuff.toString());
				bw.close();
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}
	}

	static Object getLocalCache(String key, byte[] unmarshallBuffer) {
		AtomicBusyWaitingLock lock = CoreClient.getLock(key);
		lock.lock();
		Object val = CacheHelper.get(key, unmarshallBuffer );
		lock.unlock();
//		if (val != null) {
//			hitCache.incrementAndGet();
//		}
		return val;
	}

	/**
	 * Get the value of a key that already has Q lease granted Use for REFRESH
	 * technique
	 * 
	 * @param key
	 *            The key
	 * @param clients
	 *            List of clients that may contain the value
	 * @return
	 */
	private static Object getRemoteCacheRefresh(String key,
			Map<String, Long> clients, byte[] unmarshallBuffer) {
		Object val = null;
		if (clients != null) {
			// contact other to get the value
			ArrayList<String> cidList = new ArrayList<String>(clients.keySet());
			Collections.shuffle(cidList);
			while (cidList.size() > 0 && val == null) {
				String addr = cidList.remove(0);
				if (addr.contains(CoreClient.me))
					continue;
				long fromCid = clients.get(addr);
				val = ClientBaseAction
						.copyValue(fromCid, PacketFactory.CMD_USE_ONCE, addr,
								key, unmarshallBuffer);
				if (val == null) {
					consumeNothing.incrementAndGet();
				}
			}

			if (val != null) {
				consume.incrementAndGet();
			}
		}
		return val;
	}

	@SuppressWarnings("unchecked")
	private static Object getDBMSRefresh(String query, String key,
			DoReadListener<?> doReadListener) {
		Object val = doReadListener.queryDB(query);
		queryDBFromWrite.incrementAndGet();
		
		switch (query.charAt(0)) {
		case CoreClient.LIST_FRIEND:
		case CoreClient.VIEW_PENDING:
			val = computeListOfProfileKeys((Vector<HashMap<String, ByteIterator>>) val);
			break;
		default:
			break;
		}
		return val;
	}

	protected static Object computeListOfProfileKeys(
			Vector<HashMap<String, ByteIterator>> val) {
		Vector<HashMap<String, ByteIterator>> res = (Vector<HashMap<String, ByteIterator>>) val;
		Vector<String> value = new Vector<String>();
		for (HashMap<String, ByteIterator> hm : res) {
			int id = Integer.parseInt(new String(hm.get("USERID").toArray()));
			value.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, id));
		}
		return value;
	}

	// private static Object copy(Object val) {
	// if (val instanceof HashMap<?, ?>) {
	// HashMap<String, ByteIterator> res = new HashMap<String, ByteIterator>();
	// HashMap<String, ByteIterator> v = (HashMap<String, ByteIterator>) val;
	// for (String field : v.keySet()) {
	// res.put(field, v.get(field));
	// }
	// return res;
	// } else if (val instanceof Vector<?>) {
	// Vector<HashMap<String, ByteIterator>> res = new Vector<HashMap<String,
	// ByteIterator>>();
	// Vector<HashMap<String, ByteIterator>> v = (Vector<HashMap<String,
	// ByteIterator>>) val;
	// for (HashMap<String, ByteIterator> field : v) {
	// HashMap<String, ByteIterator> hm = new HashMap<String, ByteIterator>();
	// for (String k : field.keySet()) {
	// hm.put(k, field.get(k));
	// }
	// }
	//
	// return res;
	// }
	//
	// return null;
	// }
	//
	// /**
	// * Update profile when a friend is accepted or rejected
	// *
	// * @param keys
	// * @param unmarshallBuffer
	// * @param b
	// * */
	// public static int updateProfile(int id, int delta,
	// Map<String, Map<String, Long>> keys2Clients, int requesterID,
	// DoReadListener doReadListener,
	// byte[] unmarshallBuffer, boolean fetch) {
	// if (id < 0)
	// return NOT_SUCCESS;
	//
	// String key;
	// String query;
	// if (id == requesterID) {
	// key = CoreClient.getIK(CoreClient.VIEW_PROFILE, id);
	// query = CoreClient.getQuery(CoreClient.VIEW_PROFILE, requesterID,
	// id, insertImage);
	// } else {
	// key = CoreClient.getIK(CoreClient.VIEW_PROFILE, requesterID, id);
	// query = CoreClient.getQuery(CoreClient.VIEW_PROFILE, requesterID,
	// id, insertImage);
	// }
	//
	// Object val = getLocalCache(key);
	// if (val == null && !fetch)
	// return SUCCESS;
	//
	// if (val == null) {
	// val = getRemoteCacheRefresh(key, keys2Clients.get(key),
	// unmarshallBuffer);
	// }
	//
	// if (val != null) {
	// synchronized (val) {
	// @SuppressWarnings("unchecked")
	// HashMap<String, ByteIterator> result = (HashMap<String, ByteIterator>)
	// val;
	// Set<Entry<String, ByteIterator>> set = result.entrySet();
	// Iterator<Entry<String, ByteIterator>> iterator = set.iterator();
	//
	// while (iterator.hasNext()) {
	// Entry<String, ByteIterator> entry = iterator.next();
	//
	// if (entry.getKey().equals("friendcount")) {
	// int currValue = Integer.parseInt(new String(entry
	// .getValue().toArray()));
	// currValue += delta;
	// entry.setValue(new ObjectByteIterator((currValue + "")
	// .getBytes()));
	// return SUCCESS;
	// }
	// }
	// }
	//
	// System.out.println("Error with updateProfile: userid=" + id
	// + ", delta=" + delta);
	// return NOT_SUCCESS;
	// } else {
	// val = getDBMSRefresh(query, key, doReadListener);
	// if (val != null)
	// CoreClient.getCache(key).put(key, val);
	// return SUCCESS;
	// }
	// }
	//
	// /**
	// * Update profile pending when a friend invitation is created or rejected
	// *
	// * @param keys
	// * @param unmarshallBuffer
	// * */
	// @SuppressWarnings("unchecked")
	// public static int updateProfilePending(int id, int delta,
	// Map<String, Map<String, Long>> keys2Clients,
	// DoReadListener doReadListener,
	// byte[] unmarshallBuffer, boolean updateRemote) {
	// if (id < 0)
	// return NOT_SUCCESS;
	//
	// String key = CoreClient.getIK(CoreClient.VIEW_PROFILE, id);
	// String query = CoreClient.getQuery(CoreClient.VIEW_PROFILE, id, id,
	// insertImage);
	//
	// boolean remote = false;
	// Object val = getLocalCache(key);
	// if (false && val == null) {
	// remote = true;
	// val = getRemoteCacheRefresh(key, keys2Clients.get(key),
	// unmarshallBuffer);
	// }
	//
	// if (val != null) {
	// if (!remote || updateRemote) {
	// synchronized (val) {
	// HashMap<String, ByteIterator> result = (HashMap<String, ByteIterator>)
	// val;
	// Set<Entry<String, ByteIterator>> set = result.entrySet();
	// Iterator<Entry<String, ByteIterator>> iterator = set.iterator();
	//
	// while (iterator.hasNext()) {
	// Entry<String, ByteIterator> entry = iterator.next();
	// if (entry.getKey().equals("pendingcount")) {
	// int currValue = Integer.parseInt(new String(entry
	// .getValue().toArray()));
	// currValue += delta;
	// entry.setValue(new ObjectByteIterator((currValue + "")
	// .getBytes()));
	// return SUCCESS;
	// }
	// }
	// }
	//
	//
	// System.out.println("Error with updateProfilePending: userid=" + id
	// + ", delta=" + delta);
	// return NOT_SUCCESS;
	// } else {
	// return SUCCESS;
	// }
	// } else {
	// val = getDBMSRefresh(query, key, doReadListener);
	// if (val != null)
	// CoreClient.getCache(key).put(key, val);
	// return SUCCESS;
	// }
	// }
	//
	// /**
	// * When user1 sends friend request to user2, list pending friend of user2
	// * need to be updated
	// *
	// * @param inviterID
	// * @param inviteeID
	// * return
	// * @param keys
	// * @param unmarshallBuffer
	// * @param b
	// */
	// @SuppressWarnings("unchecked")
	// public static int updatePendingFriendsInvite(int inviterID, int
	// inviteeID,
	// Map<String, Map<String, Long>> keys2Clients,
	// DoReadListener doReadListener,
	// byte[] unmarshallBuffer, boolean updateRemote) {
	// String key = CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID);
	// String query = CoreClient.getQuery(CoreClient.VIEW_PENDING, inviteeID,
	// insertImage);
	//
	// boolean remote = false;
	// Object val = getLocalCache(key);
	// if (false && val == null) {
	// remote = true;
	// val = getRemoteCacheRefresh(key, keys2Clients.get(key),
	// unmarshallBuffer);
	// }
	//
	// if (val != null) {
	// if (!remote || updateRemote) {
	// synchronized (val) {
	// Vector<String> result = (Vector<String>) val;
	// if (result.contains(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID,
	// inviterID))) {
	// System.out.println("Pending Already contain entry remote="+remote);
	// }
	// result.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID,
	// inviterID));
	// }
	// }
	// return SUCCESS;
	// } else {
	// val = getDBMSRefresh(query, key, doReadListener);
	// if (val != null)
	// CoreClient.getCache(key).put(key, val);
	// return SUCCESS;
	// }
	// }
	//
	// public static int updatePendingFriendsRemove(int inviterID, int
	// inviteeID,
	// Map<String, Map<String, Long>> keys2Clients,
	// DoReadListener doReadListener,
	// byte[] unmarshallBuffer, boolean updateRemote) {
	// String key = CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID);
	// String query = CoreClient.getQuery(CoreClient.VIEW_PENDING, inviteeID,
	// insertImage);
	// boolean remote = false;
	// Object val = getLocalCache(key);
	// if (false && val == null) {
	// remote = true;
	// val = getRemoteCacheRefresh(key, keys2Clients.get(key),
	// unmarshallBuffer);
	// }
	//
	// if (val != null) {
	// if (!remote || updateRemote) {
	// synchronized (val) {
	// @SuppressWarnings("unchecked")
	// Vector<String> result = (Vector<String>) val;
	// if (result.remove(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID,
	// inviterID))) {
	// return SUCCESS;
	// } else {
	// System.out.println("Error in updatePendingFriendsRemove: inviterID="
	// + inviterID + ", inviteeID=" + inviteeID);
	// return NOT_SUCCESS;
	// }
	// }
	// } else {
	// return SUCCESS;
	// }
	// } else {
	// val = getDBMSRefresh(query, key, doReadListener);
	// if (val != null)
	// CoreClient.getCache(key).put(key, val);
	// return SUCCESS;
	// }
	// }
	//
	// /**
	// * When user1 sends friend request to user2 and the invitation is
	// accepted,
	// * this function updates list friends of user1 by: 1. get user2
	// information
	// * 2. get cache value of user 1 3. updates cache value 4. set updated
	// value
	// * to the cache
	// *
	// * @param keys
	// * @param unmarshallBuffer
	// * @param b
	// * @param inviterID
	// * @param inviteeID
	// * return
	// */
	// @SuppressWarnings("unchecked")
	// public static int updateListFriendsAccepted(int user1, int user2,
	// Map<String, Map<String, Long>> keys2Clients,
	// DoReadListener doReadListener,
	// byte[] unmarshallBuffer, boolean updateRemote) {
	// String key = CoreClient.getIK(CoreClient.LIST_FRIEND, user1);
	// String query = CoreClient.getQuery(CoreClient.LIST_FRIEND, user2, user1,
	// insertImage);
	//
	// boolean remote = false;
	// Object val = getLocalCache(key);
	// if (false && val == null) {
	// remote = true;
	// val = getRemoteCacheRefresh(key, keys2Clients.get(key),
	// unmarshallBuffer);
	// }
	//
	// if (val != null) {
	// if (!remote || updateRemote) {
	// synchronized (val) {
	// Vector<String> result = (Vector<String>) val;
	// if (result.contains(CoreClient.getIK(CoreClient.VIEW_PROFILE, user1,
	// user2))) {
	// System.out.println("Already contain entry remote="+remote);
	// }
	// result.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, user1, user2));
	// }
	// }
	// return SUCCESS;
	// } else {
	// val = getDBMSRefresh(query, key, doReadListener);
	// if (val != null)
	// CoreClient.getCache(key).put(key, val);
	// return SUCCESS;
	// }
	// }
	//
	// @SuppressWarnings("unchecked")
	// public static int updateListFriendsRemove(int user1, int user2,
	// Map<String, Map<String, Long>> keys2Clients,
	// DoReadListener doReadListener,
	// byte[] unmarshallBuffer, boolean updateRemote) {
	// String key = CoreClient.getIK(CoreClient.LIST_FRIEND, user1);
	// String query = CoreClient.getQuery(CoreClient.LIST_FRIEND, user2,
	// user1, insertImage);
	// boolean remote = false;
	// Object val = getLocalCache(key);
	// if (false && val == null) {
	// remote = true;
	// val = getRemoteCacheRefresh(key, keys2Clients.get(key),
	// unmarshallBuffer);
	// }
	//
	// if (val != null) {
	// if (!remote || updateRemote) {
	// synchronized (val) {
	// Vector<String> result = (Vector<String>) val;
	// if (result.remove(CoreClient.getIK(CoreClient.VIEW_PROFILE, user1,
	// user2))) {
	// return SUCCESS;
	// } else {
	// System.out.println("Error in updateListFriendsRemove");
	// return NOT_SUCCESS;
	// }
	// }
	// } else {
	// return SUCCESS;
	// }
	// } else {
	// val = getDBMSRefresh(query, key, doReadListener);
	// if (val != null)
	// CoreClient.getCache(key).put(key, val);
	// return SUCCESS;
	// }
	// }
	
	public static void resetStats() {
		System.out.println("Reset Stats");
		hitCache.set(0);
		totalNumberOfReads.set(0);
		backoffReads.set(0);
		backoffWrites.set(0);
		failReleasedReads.set(0);
		releasedItem.set(0);
		queryDB.set(0);
		queryDBFromRead.set(0);
		totalNumberOfWrites.set(0);
		steal.set(0);
		stolen.set(0);
		stealNothing.set(0);
		stealFromMe.set(0);
		stolenFromMe.set(0);
		consume.set(0);
		consumed.set(0);
		consumeNothing.set(0);
		consumeFromMe.set(0);
		consumedFromMe.set(0);
		getFromMyself.set(0);
		updateFail.set(0);
		numSendQLeases.set(0);
		numberOfReadPCFriends.set(0);
		avrPCFriends.set(0);
		numUpdateMsgs.set(0);
	}
}
