package kosar;

import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;

public class DoReadTokenWorker extends TokenWorker {
	DB db;
	private Semaphore semaphore;
	private ConcurrentLinkedQueue<DoReadJob> job;
	
	public DoReadTokenWorker(int workerID, DB db, 
			Semaphore semaphore, ConcurrentLinkedQueue<DoReadJob> job) {
		super(workerID);
		this.db = db;
		this.semaphore = semaphore;
		this.job = job;
	}
	
	@Override
	public void doWork(TokenObject token) {
		DoReadJob job = (DoReadJob)token;
		String key = job.getKey();
		int userid = Integer.parseInt(key.substring(1, key.length()));
		int requesterid = 0;
		
		HashMap<String, ByteIterator> hm = new HashMap<String, ByteIterator>();
		Vector<HashMap<String, ByteIterator>> vt = new Vector<HashMap<String, ByteIterator>>();
		switch (key.charAt(0)) {
		case CoreClient.VIEW_PROFILE:
			db.viewProfile(requesterid, userid, hm, false, false);
			break;
		case CoreClient.LIST_FRIEND:
			db.listFriends(requesterid, userid, null, vt, false, false);
			break;
		case CoreClient.VIEW_PENDING:
			db.viewFriendReq(userid, vt, false, false);
			break;
		}
	}
	
	@Override
	public void run() {
		while (true) {
			try {
				semaphore.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			DoReadJob j = job.poll();
			if (j == null)
				System.out.println("Got a null job");
			
			doWork(j);
		}
	}
}