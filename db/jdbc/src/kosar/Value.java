package kosar;

import java.util.Map;
import java.util.Vector;

/**
 * Represents a value
 * @author hieun
 *
 */
public class Value {
	Object val;
	Vector<Delta> deltas;
	
	public Value(Object val) {
		this.val = val;
		this.deltas = new Vector<>();
	}
	
	public Value(Object val, Vector<Delta> deltas) {
		this.val = val;
		this.deltas = deltas;
	}
	
	public Object get() {
		return val;
	}
	
	public void addDelta(Delta delta) {
		this.deltas.addElement(delta);
		this.val = compute(val, delta);
	}
	
	public boolean commit(long clientId, int transactionId) {
		if (deltas.size() == 0)
			return false;
		
		for (Delta delta: deltas) {
			if (delta.verify(clientId, transactionId) == true) {
				deltas.remove(delta);
				return true;
			}
		}
		
		System.out.println(String.format("Cannot verify delta "
				+ "clientId %d transactionId %d", 
				clientId, transactionId));
		
		return false;
	}
	
	public boolean abort(long clientId, int transactionId) {
		if (deltas.size() == 0)
			return false;
		
		Delta delta = null;
		for (Delta d: deltas) {
			if (d.verify(clientId, transactionId) == true) {
				delta = d;
				break;
			}
		}
		
		if (delta == null) {
			System.out.println(String.format("Cannot verify delta "
					+ "clientId %d transactionId %d", 
					clientId, transactionId));
			return false;
		}
		
		Object value = deepCopy(val);
		deCompute(value, deltas);
		deltas.remove(delta);
		reCompute(value, deltas);
		return true;		
	}
	
	private Delta getReverse(Delta delta) {
		int op = 0;
		switch (delta.getOp()) {
		case Delta.ADD:
			op = Delta.RMV;
			break;
		case Delta.RMV:
			op = Delta.ADD;
			break;
		case Delta.INCR:
			op = Delta.DECR;
			break;
		case Delta.DECR:
			op = Delta.INCR;
			break;
		}
		
		return new Delta(delta.getClientId(), delta.getTransactionId(), 
				op, delta.getData());
	}
	
	private Object deCompute(Object value, Vector<Delta> deltas) {
		for (int i = deltas.size() - 1; i >= 0; i--) {
			Delta d = getReverse(deltas.get(i));
			compute(value, d);
		}
		return value;
	}

	private Object reCompute(Object value, Vector<Delta> deltas) {
		for (Delta d: deltas) {			
			compute(value, d);
		}
		return value;
	}

	private Object deepCopy(Object commitedVal2) {
		// TODO Auto-generated method stub
		return null;
	}

	public static Object compute(Object val, Delta delta) {
		switch (delta.getOp()) {
		case Delta.ADD:
			D.add("", val, delta.getData());
			break;
		case Delta.RMV:
			D.rmv("", val, delta.getData());
			break;
		case Delta.INCR:
			Map<String, Integer> map = (Map<String, Integer>)delta.getData();
			for (String attr: map.keySet()) {
				D.incr(val, attr, map.get(attr));
			}
			break;
		case Delta.DECR:
			map = (Map<String, Integer>)delta.getData();
			for (String attr: map.keySet()) {
				D.decr(val, attr, map.get(attr));
			}
			break;
		}
		
		return val;
	}
}
