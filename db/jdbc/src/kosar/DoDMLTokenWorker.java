package kosar;

import java.util.HashMap;
import java.util.Map;

public class DoDMLTokenWorker extends TokenWorker {
	public DoDMLTokenWorker(int workerID) {
		super(workerID);
	}

	@Override
	public void doWork(TokenObject token) {
		// TODO Auto-generated method stub
		if (DoDMLJob.class.isInstance(token)) {
			DoDMLJob job = DoDMLJob.class.cast(token);
			switch(job.jobType) {
			case INVALIDATE:
				int ret = ClientBaseAction.invalidateNotification(job.cid, job.serverAddr, job.tid, job.iks, null);
				if (ret == -1) {
					System.out.println(String.format("Q lease rejected %d ", job.tid));
				}
				break;
			case RELEASE:
				if (job.txResult == 0) {
					// commit
					while (ClientBaseAction.commitTID(job.cid, job.serverAddr,
							job.tid, null) == PacketFactory.CMD_REQUEST_ABORTED)
						;
				} else {
					while (ClientBaseAction.abortTID(job.cid, job.serverAddr,
							job.tid, null) == PacketFactory.CMD_REQUEST_ABORTED)
						;
				}
				break;
			case ACQUIRE_Q_LEASE_REFRESH:		
				HashMap<String, Map<String, Long>> hm = new HashMap<String, Map<String, Long>>();
//				while (true) {					
					ret = ClientBaseAction.acquireQLease(job.cid, job.serverAddr, job.tid, job.iks, hm);
					if (ret == -1) {
//						System.out.println(String.format("Q lease rejected %d ", job.tid));
						ClientAction.backoffWrites.incrementAndGet();
//						try {
//							Thread.sleep(CoreClient.backOffTime);
//						} catch (InterruptedException e) {
//							// TODO Auto-generated catch block
//							e.printStackTrace();
//						}
//						continue;
					}
//					break;
//				}
				job.setResult(ret);
				job.setKeys2Clients(hm);
				break;
			case RELEASE_Q_LEASE_REFRESH:
				ret = ClientBaseAction.releaseQLease(job.cid, job.serverAddr, job.tid, job.iks);
				job.setResult(ret);
				break;
			case ABORT_Q_LEASE_REFRESH:
				ret = ClientBaseAction.abortQLeaseRefresh(job.cid, job.serverAddr, job.tid);
				if (ret == -1) {
					System.out.println(String.format("Q lease abort unsuccesful", job.tid));
				}
				break;
			case REQUEST_UPDATE:
				ret = ClientBaseAction.requestUpdate(job.cid, job.tid, job.serverAddr, job.query, job.iks, job.keys2clients);
				break;
			default:
				break;
			}
			
			job.finishJob();
		} else {
			System.out.println("don't know this job");
		}
	}

}
