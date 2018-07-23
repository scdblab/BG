package kosar;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class DoDMLJob extends TokenObject {
	public static enum JobType {
		INVALIDATE, RELEASE, ACQUIRE_Q_LEASE_REFRESH, 
		RELEASE_Q_LEASE_REFRESH, ABORT_Q_LEASE_REFRESH,
		REQUEST_UPDATE
	}
	
	JobType jobType;
	
	long cid;
	
	String serverAddr;
	
	int tid;
	
	Set<String> iks;
	
	int txResult;
	
	Semaphore latch;
	
	AtomicInteger jobCounts;
	
	String query;
	
	int result;
	Map<String, Map<String, Long>> keys2clients;

	public DoDMLJob(JobType jobType, long cid, String serverAddr, int tid,
			Set<String> iks, int txResult, Semaphore latch,
			AtomicInteger jobCounts) {
		super();
		this.jobType = jobType;
		this.cid = cid;
		this.serverAddr = serverAddr;
		this.tid = tid;
		this.iks = iks;
		this.txResult = txResult;
		this.latch = latch; 
		this.jobCounts = jobCounts;
		
		result = -1; 
		keys2clients = new HashMap<String, Map<String, Long>>();
	}
	
	public DoDMLJob(JobType jobType, long cid, int tid, String serverAddr, String query, Set<String> iks, 
			Map<String, Map<String, Long>> keys2Clients, Semaphore latch, AtomicInteger jobCounts) {
		super();
		this.cid = cid;
		this.tid = tid;
		this.jobType = jobType;
		this.serverAddr = serverAddr;
		this.iks = iks;
		this.query = query;
		this.jobCounts = jobCounts;
		this.latch = latch;
		
		result = -1;
		this.keys2clients = keys2Clients;
	}
	
	public void setJobType(JobType type) {
		this.jobType = type;
	}
	
	public void setLatch(Semaphore latch) {
		this.latch = latch;
	}
	
	public void setJobCounts(AtomicInteger jobCounts) {
		this.jobCounts = jobCounts;
	}

	public void startJob() {
		try {
			this.latch.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void finishJob() {
		if (this.jobCounts.decrementAndGet() == 0) {
			this.latch.release();
		}
	}
	
	public String getServerAddress() {
		return serverAddr;
	}
	
	public int getResult() { return result; }
	public Map<String, Map<String, Long>> getKeys2Clients() {
		return keys2clients;
	}
	
	public void setResult(int result) { this.result = result; }  
	public void setKeys2Clients(HashMap<String, Map<String, Long>> hm) {
		keys2clients = hm;
	}
}
