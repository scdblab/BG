package kosar;

public class Delta {
	public static final int INCR = 1, DECR = 2, ADD = 3, RMV = 4;
	private int op;
	private Object data;
	private long clientId;
	private int transactionId;
	
	public Delta(long clientId, int transactionId, int op, Object data) { 
		this.op = op; 
		this.data = data; 
		this.clientId = clientId;
		this.transactionId = transactionId;
	}
	
	public int getOp() {
		return op;
	}
	
	public Object getData() {
		return data;
	}
	
	public boolean verify(long clientId, int transactionId) {
		return (this.clientId == clientId && this.transactionId == transactionId); 
	}
	
	public long getClientId() { return clientId; }
	public int getTransactionId() { return transactionId; }
}
