package kosar;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Vector;

import common.CacheUtilities;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.dblab.cm.base.CacheManager;

class KosarValue extends edu.usc.dblab.cm.base.Value {
	private static final int HASHMAP = 1;
	private static final int VECTOR = 2;
	
	public KosarValue(Object val) {
		super(val);
	}
	
	@Override
	public byte[] serialize() {
		byte[] bytes = null;
		
		if (val instanceof HashMap<?, ?>) {
			bytes = CacheUtilities.SerializeHashMap((HashMap<String, ByteIterator>)val);
			ByteBuffer wrap = ByteBuffer.wrap(new byte[bytes.length+4]);
			wrap.putInt(HASHMAP);
			wrap.put(bytes, 0, bytes.length);
			return wrap.array();
		} else if (val instanceof Vector<?>) {
			bytes = CacheUtilities.SerializeVectorOfStrings((Vector<String>)val);
			ByteBuffer wrap = ByteBuffer.wrap(new byte[bytes.length+4]);
			wrap.putInt(VECTOR);
			wrap.put(bytes, 0, bytes.length);
			return wrap.array();
		} else if (val instanceof byte[]) {
			return (byte[])val;
		}
		
		return null;
	}
	
	@Override
	public Object deserialize(byte[] buffer) {
		if (!(val instanceof byte[]))
			return val;
		byte[] val_bytes = (byte[]) val;
		ByteBuffer wrap = ByteBuffer.wrap(val_bytes);
		int ds = wrap.getInt();
		byte[] bytes = new byte[val_bytes.length-4];
		wrap.get(bytes);
		
		switch (ds) {
		case HASHMAP:
			HashMap<String, ByteIterator> hm = new HashMap<String, ByteIterator>();
			CacheUtilities.unMarshallHashMap(hm, bytes, buffer);
			return hm;
		case VECTOR:
			Vector<String> v = new Vector<String>();
			CacheUtilities.unMarshallVectorOfStrings(bytes, v, buffer);
			return v;
		default:
			return null;
		}
	}
}

public class CacheHelper {
	public static boolean usingDeltaCommit = false;
	public static boolean useCopy = false;
	public static CacheManager cm;
	
	public static Object get(String key, byte[] buffer) {
		Object obj = null;
		
		if (!usingDeltaCommit) { 
			edu.usc.dblab.cm.base.Value v = cm.getKV(key, buffer);
			
			if (v != null) {
				if (v.isSerialize()) {
					obj = v.deserialize(buffer);
				} else {	
					obj = v.getVal();
				}
				
				if (useCopy) {
					obj = deepCopy(key, obj);
				}
			}
		} else {
			obj = ((Value)((edu.usc.dblab.cm.base.Value)cm.getKV(key, buffer)).getVal()).get();
		}
		
		return obj;
	}
	
	public static Object get(String key) {
		Object obj = null;
		
		if (!usingDeltaCommit) { 
			obj = cm.getKV(key).getVal();
		} else {
			obj = ((Value)cm.getKV(key).getVal()).get();
		}
		
		return obj;
	}
	
	public static byte[] getSerialized(String key) {
		if (!usingDeltaCommit) { 
			edu.usc.dblab.cm.base.Value v = cm.getKV(key);
			
			if (v != null) {
				if (v.isSerialize()) {
					return (byte[])v.getVal();
				} else {	
					return v.serialize();
				}
			}
		} else {
			// TODO
			return null;	// not handle yet
		}
		
		return null;
	}

	@SuppressWarnings("unchecked")
	private static Object deepCopy(String key, Object obj) {
		switch (key.charAt(0)) {
		case CoreClient.VIEW_PROFILE:
			HashMap<String, ByteIterator> hm = (HashMap<String, ByteIterator>) obj;
			HashMap<String, ByteIterator> copyHm = new HashMap<String, ByteIterator>();
			ObjectByteIterator.deepCopy(hm, copyHm);
			return copyHm;
		case CoreClient.VIEW_PENDING:
		case CoreClient.LIST_FRIEND:
			Vector<String> profileKeys = (Vector<String>) obj;
			Vector<String> copyProfileKeys = new Vector<String>();
			synchronized (obj) {
				for (int i = 0; i < profileKeys.size(); i++) {
					copyProfileKeys.add(profileKeys.get(i));
				}
			}
			return copyProfileKeys;
		}
		
		return null;
	}

	public static Object deserialize(byte[] bytes, byte[] buffer) {
		KosarValue v = new KosarValue(bytes);
		return v.deserialize(buffer);
	}

	public static boolean updateCache(Object val, Delta d) {
		if (!usingDeltaCommit) {
			Value.compute(val, d);
			return true;
		} else {
			((Value)val).addDelta(d);
			return true;
		}
	}

//	public static boolean commitChange(String key, long clientId, 
//			int transactionId, int result) {
//		Value v = (Value) get(key);
//		if (result == 0)
//			return v.commit(clientId, transactionId);
//		else
//			return v.abort(clientId, transactionId);
//	}

	public static boolean set(String key, Object value) {
		KosarValue v = new KosarValue(value);
		
		if (!usingDeltaCommit) {
			cm.insertKV(key, v);
		} else {
			Value val = new Value(value);
			v = new KosarValue(val);
			cm.insertKV(key, v);
		}

		return true;
	}
 
//	@SuppressWarnings("unchecked")
//	public static byte[] serialize(String key, Object value) {
//		byte[] bytes = null;
//		
//		switch (key.charAt(0)) {
//		case CoreClient.VIEW_PROFILE: 
//			bytes = CacheUtilities.SerializeHashMap((HashMap<String, ByteIterator>)value);
//			break;
//		case CoreClient.VIEW_PENDING:
//		case CoreClient.LIST_FRIEND:
//			bytes = CacheUtilities.SerializeVectorOfStrings((Vector<String>)value);
//			break;
//		}
//		
//		return bytes;
//	}

	public static Object delete(String key, boolean serialized) {		
		edu.usc.dblab.cm.base.Value value = cm.deleteKV(key);
		if (value == null) return null;
		if (serialized && !value.isSerialize())
			return value.serialize();
		return value.getVal();
	}

	public static long getUsedSpace() {
		Runtime rt = Runtime.getRuntime();
		return rt.totalMemory() - rt.freeMemory();
	}
}