package KosarIQClient;

import java.util.HashMap;
import java.util.Vector;

import edu.usc.IQ.client.ValueListener;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class ValueListenerImpl<V> implements ValueListener<V> {

	private final DB db;
	
	private byte[] buffer;
	
	public ValueListenerImpl(DB db) {
		super();
		this.db = db;
	}
	
	public ValueListenerImpl(DB db, byte[] buffer) {
		super();
		this.db = db;
		this.buffer = buffer;
	}

	@Override
	public V deserialize(String query, byte[] payload) {
		Object res = null;
		if (this.buffer == null || this.buffer.length < payload.length) {
			this.buffer = new byte[payload.length];
		}
		if (IQClientWrapper.VIEW_PROFILE == query.charAt(0)) {
			res = new HashMap<String, ByteIterator>();
			common.CacheUtilities.unMarshallHashMap(
					(HashMap<String, ByteIterator>) res, payload,
					this.buffer);
		} else if (IQClientWrapper.LIST_FRIEND == query.charAt(0)
				|| IQClientWrapper.VIEW_PENDING == query.charAt(0)) {
			res = new Vector<HashMap<String, ByteIterator>>();
			common.CacheUtilities.unMarshallVectorOfHashMaps(payload,
					(Vector<HashMap<String, ByteIterator>>) res,
					this.buffer);
		}
		return (V) res;
	}

	@Override
	public byte[] serialize(String query, V value) {
		byte[] buf = null;
		if (IQClientWrapper.VIEW_PROFILE == query.charAt(0)) {
			HashMap<String, ByteIterator> copy = new HashMap<String, ByteIterator>();
			ObjectByteIterator.deepCopy((HashMap<String, ByteIterator>) value,
					copy);
			buf = common.CacheUtilities.SerializeHashMap(copy);
		} else if (IQClientWrapper.LIST_FRIEND == query.charAt(0)
				|| IQClientWrapper.VIEW_PENDING == query.charAt(0)) {
			Vector<HashMap<String, ByteIterator>> copy = new Vector<HashMap<String, ByteIterator>>();
			Vector<HashMap<String, ByteIterator>> res = (Vector<HashMap<String, ByteIterator>>) value;
			for (HashMap<String, ByteIterator> em : res) {
				HashMap<String, ByteIterator> c = new HashMap<String, ByteIterator>();
				ObjectByteIterator.deepCopy(em, c);
				copy.add(c);
			}
			buf = common.CacheUtilities.SerializeVectorOfHashMaps(copy);
		}
		return buf;
	}

	public V query(String query) {
		int requestId = -1;
		int profileId = -1;
		boolean insertImage = false;
		Object result = null;
//		System.out.println(query);
		String[] em = query.split(",");
		int val = -1;
		switch (query.charAt(0)) {
		case IQClientWrapper.VIEW_PROFILE:
			requestId = Integer.parseInt(em[1]);
			profileId = Integer.parseInt(em[2]);
			insertImage = Boolean.parseBoolean(em[3]);
			result = new HashMap<String, ByteIterator>();
			while (true) {
				val = this.db.viewProfile(requestId, profileId,
						(HashMap<String, ByteIterator>) result, insertImage, false);
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
		case IQClientWrapper.LIST_FRIEND:

			requestId = Integer.parseInt(em[1]);
			profileId = Integer.parseInt(em[2]);
			insertImage = Boolean.parseBoolean(em[3]);

			result = new Vector<HashMap<String, ByteIterator>>();
			while (true) {
				val = this.db.listFriends(requestId, profileId, null,
						(Vector<HashMap<String, ByteIterator>>) result,
						insertImage, false);
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
		case IQClientWrapper.VIEW_PENDING:
			profileId = Integer.parseInt(em[1]);
			insertImage = Boolean.parseBoolean(em[2]);
			result = new Vector<HashMap<String, ByteIterator>>();
			while (true) {
				val = this.db.viewFriendReq(profileId,
						(Vector<HashMap<String, ByteIterator>>) result,
						insertImage, false);
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
		return (V) result;
	}

}
