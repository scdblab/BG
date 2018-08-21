package kosar;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import relational.JdbcDBClient;

public class DBWorker implements Runnable {
	public static final int WORKER_SIZE = 13;

	@SuppressWarnings("unchecked")
	public static ConcurrentLinkedQueue<DBJob>[] jobQueue = new ConcurrentLinkedQueue[WORKER_SIZE];
	public static Semaphore[] semaphores = new Semaphore[WORKER_SIZE];

	public static AtomicInteger jobCount = new AtomicInteger(0);
	public static int[] maxQueueLength = new int[WORKER_SIZE];

	private int id;
	private boolean stop = false;
	public static Random rand = new Random();

	public static final int MAX_BATCH_STMT = 100;
	public static final boolean batch = false;

	public static void addJob(DBJob job, long l) {
		long idx = l % WORKER_SIZE;
		jobQueue[(int) idx].add(job);
		jobCount.incrementAndGet();

		semaphores[(int) idx].release();
	}

	public static void addJob(DBJob job) {
		String query = job.getQuery();
		String[] ems = query.split(",");
		int idx;

		if (ems == null) {
			System.out.println(String.format("ems is null for query %s", query));
			idx = rand.nextInt(WORKER_SIZE);
		} else {
			int inviter = Integer.parseInt(ems[1]);
			int invitee = Integer.parseInt(ems[2]);
			int i = inviter + invitee;
			idx = i % WORKER_SIZE;
		}

		jobQueue[idx].add(job);
		jobCount.incrementAndGet();
		semaphores[idx].release();
	}

	DB db;
	Statement stmt;

	public DBWorker(int id, Properties p) {
		this.id = id;
		db = new JdbcDBClient();
		db.setProperties(p);
		try {
			db.getProperties();
			db.init();
		} catch (DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			stmt = db.getConnection().createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
	}

	@Override
	public void run() {
		while (!stop) {
			if (batch) {
				executeDMLs();
			} else {
				executeDML();
			}

			if (stop)
				break;
		}
	}

	private void executeDML() {
		try {
			semaphores[id].acquire();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		DBJob job = jobQueue[id].poll();
		if (job == null)
			System.out.println("Got a null job");
		else {
			int currLen = jobQueue[id].size();
			if (currLen > maxQueueLength[id])
				maxQueueLength[id] = currLen;

			job.execute(db);
		}

		job.finish();
	}

	private void executeDMLs() {
		Connection conn = db.getConnection();
		boolean first = true;
		int cnt = 0;

		List<DBJob> jobs = new ArrayList<DBJob>();
		try {
			conn.setAutoCommit(false);

			while (cnt < MAX_BATCH_STMT) {
				boolean acquire = true;
				if (first) {
					try {
						semaphores[id].acquire();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					first = false;
				} else {
					acquire = semaphores[id].tryAcquire();
				}

				if (acquire) {
					DBJob job = jobQueue[id].poll();
					int currLen = jobQueue[id].size();
					if (currLen > maxQueueLength[id])
						maxQueueLength[id] = currLen;
					if (job == null) {
						System.out.println("Got a null job");
					} else {
						jobs.add(job);
						String query = job.getQuery();
						switch (query.charAt(0)) {
						case CoreClient.VIEW_PROFILE:
						case CoreClient.VIEW_PENDING:
						case CoreClient.LIST_FRIEND:
							System.out.println("Error: not an dml");
							return;
						case CoreClient.INVITE:
						case CoreClient.REJECT:
						case CoreClient.ACCEPT:
						case CoreClient.THAW:
							String[] ems = query.split(",");
							int inviter = Integer.parseInt(ems[1]);
							int invitee = Integer.parseInt(ems[2]);
							stmt.addBatch(ClientAction.getDMLFromQuery(query.charAt(0), inviter, invitee));
							break;
						}
					}
				} else {
					break;
				}
			}

			stmt.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.clearBatch();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace(System.out);
				}

			try {
				conn.setAutoCommit(true);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(System.out);
			}
		}

		// finish job
		for (DBJob job : jobs) {
			job.finish();
		}
	}
}
