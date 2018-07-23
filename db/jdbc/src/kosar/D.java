package kosar;

import java.util.HashMap;
import java.util.Vector;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.ObjectByteIterator;

public class D {
	@SuppressWarnings("unchecked")
	public static void incr(Object val, String attr, int d) {
		if (val == null) return;
		synchronized (val) {
			HashMap<String, ByteIterator> hm = (HashMap<String, ByteIterator>) val;
			int x = Integer.parseInt(new String(hm.get(attr).toArray()));
			hm.put(attr, new ObjectByteIterator((x+d+"").getBytes()));
		}
	}

	@SuppressWarnings("unchecked")
	public static void decr(Object val, String attr, int d) {
		if (val == null) return;
		synchronized (val) {
			HashMap<String, ByteIterator> hm = (HashMap<String, ByteIterator>) val;
			int x = Integer.parseInt(new String(hm.get(attr).toArray()));
			hm.put(attr, new ObjectByteIterator((x-d+"").getBytes()));
		}
	}	

	@SuppressWarnings("unchecked")
	public static boolean add(String key, Object val, Object d) {
		if (val == null) return false;
		synchronized (val) {
			Vector<String> vt = (Vector<String>)val;
			if (vt.contains(d)) {
				System.out.println("Error: add vt key= "+key+" already contains d "+d);
				for (String s: vt) {
					System.out.print(s + " ");
				}
				System.out.println();
				return false;
			}

			vt.add((String)d);
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	public static boolean rmv(String key, Object val, Object d) {
		if (val == null) return false;
		
		boolean remove;
		synchronized (val) {
			Vector<String> vt = (Vector<String>)val;
			remove = vt.remove(d);
			if (!remove) {
				System.out.println("Error: rmv vt key= "+key+" does not contain d "+d);
				for (String s: vt) {
					System.out.print(s + " ");
				}
				System.out.println();
			}
		}

		return remove;
	}
}
