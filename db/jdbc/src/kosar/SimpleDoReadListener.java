package kosar;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import kosar.AbstractDoReadListener;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.bg.base.WorkloadException;
import edu.usc.bg.workloads.Test;
import edu.usc.bg.workloads.UserWorkload;
import MySQL.AggrAsAttr2R1TMySQLClient;

public class SimpleDoReadListener extends AbstractDoReadListener<Object> {

	private DB db; 
//	private AggrAsAttr2R1TMySQLClient db;
	
	public static AtomicInteger numViewProfileQueries = new AtomicInteger(0);
	public static AtomicInteger numListFriendsQueries = new AtomicInteger(0);
	public static AtomicInteger numViewPendingQueries = new AtomicInteger(0);
	public static AtomicInteger numFriendCountQueries = new AtomicInteger(0);
	public static AtomicInteger numPendingCountQueries = new AtomicInteger(0);
	
	public static final boolean FAKE_DATA = true;
	private UserWorkload userWorkload;
	Method m;
	private int numFriendsPerUser = 100;
	private int numUsers = 100000;
	private Random rand = new Random();

	public SimpleDoReadListener(DB db) {
		super();
		this.db = db;
		if (FAKE_DATA) {
			numFriendsPerUser = Integer.parseInt(db.getProperties().getProperty("friendcountperuser"));
			numUsers = Integer.parseInt(db.getProperties().getProperty("usercount"));
			this.userWorkload = new UserWorkload();
			try {
				userWorkload.init(db.getProperties(), null);
			} catch (WorkloadException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			try {
				m = UserWorkload.class.getDeclaredMethod("buildValues");
				m.setAccessible(true);
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	@Override
	public Object queryDB(String query) {
		ClientAction.queryDB.incrementAndGet();
		int requestId = -1;
		int profileId = -1;
		boolean insertImage = true;
		Object result = null;
		String[] em = query.split(",");
		int val = -1;
		switch (query.charAt(0)) {
		case CoreClient.VIEW_PROFILE:
			numViewProfileQueries.incrementAndGet();
			boolean simplified = false;
			char s = query.charAt(query.length()-1);		// get final char
			if (s == 'S')
				simplified = true;		
			
			requestId = Integer.parseInt(em[1]);
			profileId = Integer.parseInt(em[2]);
			insertImage = Boolean.parseBoolean(em[3]);
			result = new HashMap<String, ByteIterator>();
			while (true) {
//				val = this.db.viewProfile(requestId, profileId,
//						(HashMap<String, ByteIterator>) result, insertImage, false, simplified);
				if (FAKE_DATA) {
					try {
						HashMap<String, ByteIterator> hm = (HashMap<String, ByteIterator>) m.invoke(userWorkload);
//						HashMap<String, ByteIterator> hm = Test.buildValues();
						hm.put("USERID", new ObjectByteIterator((profileId+"").getBytes()));
						hm.put("pendingcount", new ObjectByteIterator("0".getBytes()));
						hm.put("friendcount", new ObjectByteIterator((numFriendsPerUser+"").getBytes()));
						hm.put("ResCount", new ObjectByteIterator("0".getBytes()));
						result = hm;
//						result = userWorkload.buildValues();
					} 
					catch (IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
					catch (IllegalArgumentException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} 
					catch (InvocationTargetException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					val = 0;
				} else {
					val = this.db.viewProfile(requestId, profileId,
						(HashMap<String, ByteIterator>) result, insertImage, false);
				}
				
				if (val == -3) {
					try {
						this.db.cleanup(true);
						while (!this.db.init());
					} catch (DBException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
			break;
		case CoreClient.LIST_FRIEND:
			numListFriendsQueries.incrementAndGet();
			requestId = Integer.parseInt(em[1]);
			profileId = Integer.parseInt(em[2]);
			insertImage = Boolean.parseBoolean(em[3]);

			result = new Vector<HashMap<String, ByteIterator>>();
			while (true) {
				if (FAKE_DATA) {
					result = fakeVectorOfHashMap(profileId, numFriendsPerUser);
					val = 0;
				} else {
					val = this.db.listFriends(requestId, profileId, null,
							(Vector<HashMap<String, ByteIterator>>) result,
							insertImage, false);
				}
				if (val == -3) {
					try {
						this.db.cleanup(true);
						while (!this.db.init());
					} catch (DBException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
			break;
		case CoreClient.VIEW_PENDING:
			numViewPendingQueries.incrementAndGet();
			profileId = Integer.parseInt(em[1]);
			insertImage = Boolean.parseBoolean(em[2]);
			result = new Vector<HashMap<String, ByteIterator>>();
			while (true) {
				if (FAKE_DATA) {
					result = fakeVectorOfHashMap(profileId, 0);
					val = 0;
				} else {
					val = this.db.viewFriendReq(profileId,
							(Vector<HashMap<String, ByteIterator>>) result,
							insertImage, false);
				}
				if (val == -3) {
					try {
						this.db.cleanup(true);
						while (!this.db.init());
					} catch (DBException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
			break;
		case CoreClient.FRIEND_COUNT:
			numFriendCountQueries.incrementAndGet();
			profileId = Integer.parseInt(em[1]);
			result = new StringBuffer();
			while (true) {
				if (db instanceof JdbcDBClient)
					val = ((JdbcDBClient)this.db).friendCount(profileId, (StringBuffer)result);
				else if (db instanceof AggrAsAttr2R1TMySQLClient)
					val = ((AggrAsAttr2R1TMySQLClient)this.db).friendCount(profileId, (StringBuffer)result);
				
				if (val == -3) {
					try {
						this.db.cleanup(true);
						while (!this.db.init());
					} catch (DBException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
			break;
		case CoreClient.PENDING_COUNT:
			numPendingCountQueries.incrementAndGet();
			profileId = Integer.parseInt(em[1]);
			result = new StringBuffer();
			while (true) {
				if (db instanceof JdbcDBClient)
					val = ((JdbcDBClient)this.db).pendingCount(profileId, (StringBuffer)result);
				else if (db instanceof AggrAsAttr2R1TMySQLClient)
					val = ((AggrAsAttr2R1TMySQLClient)this.db).friendCount(profileId, (StringBuffer)result);
				
				if (val == -3) {
					try {
						this.db.cleanup(true);
						while (!this.db.init());
					} catch (DBException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
			break;
		default:
			System.out.println("no idea " + query);
			break;
		}
		if (val < 0) {
			return null;
		}
		return result;
	}

	private Vector<HashMap<String, ByteIterator>> fakeVectorOfHashMap(int profileId, int length) {
		Vector<HashMap<String, ByteIterator>> v = new Vector<HashMap<String, ByteIterator>>();
		int part = numUsers / 8;
		int min = (profileId / part) * part;
		int max = min + (part -1);
		for (int i = 1; i <= length/2; i++) {
			int id = profileId+i > max ? profileId+i-part: profileId+i;
			HashMap<String, ByteIterator> hm = new HashMap<String, ByteIterator>();
			hm.put("USERID", new ObjectByteIterator((id+"").getBytes()));
			v.add(hm);
			
			id = profileId-i < min ? profileId-i+part: profileId-i;
			hm = new HashMap<String, ByteIterator>();
			hm.put("USERID", new ObjectByteIterator((id+"").getBytes()));
			v.add(hm);
		}
		return v;
	}

	@Override
	public Object cast(Object value) {
		return value;
	}

	@Override
	public void setDB(DB db) {
		this.db = db;
	}

}
