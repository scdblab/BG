package kosar;

import java.net.ConnectException;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class SockIOPool {
	private static final int LIMIT = 512;
	public Semaphore poolSemaphore;
	public ConcurrentLinkedQueue<SocketIO> availPool;
	private int initConn = 10;
	private String server;

	public SockIOPool(String server, int con) {
		this.server = server;
		initConn = con;
		if (null == availPool)
			availPool = new ConcurrentLinkedQueue<SocketIO>();
		poolSemaphore = new Semaphore(con);
		for (int j = 0; j < initConn; j++) {
			SocketIO socket = null;
			while (socket == null) {
				try {
					socket = createSocket(server);
				}  catch (Exception e) {
					System.out.println(String.format("ip %s %s", this.server, e.getMessage()));
				}
			}
			this.availPool.add(socket);
		}
	}

	public static SocketIO createSocket(String host) throws Exception {
		SocketIO socket = null;
		try {
			String[] ip = host.split(":");
			Socket so = new Socket(ip[0], Integer.parseInt(ip[1]));
			socket = new SocketIO(so);
		} catch (ConnectException c) {
			throw c;
		} catch (Exception ex) {
			socket = null;
			throw ex;
		}
		return socket;
	}

	public void shutdownPool() {
		// if (availPool.size() != initConn) {
		// System.out
		// .println("DB pool size not match the number of DB connections");
		// System.exit(0);
		// }
		// Iterator<SocketIO> i = availPool.keySet().iterator();
		// while (i.hasNext()) {
		// try {
		// i.next().closeAll();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		// i.remove();
		// }
		// availPool = null;
		// poolSemaphore = null;
	}

	public SocketIO getConnection() {
		try {
			boolean result = poolSemaphore.tryAcquire();
			if (!result) {
				synchronized(this.availPool) {
					result = poolSemaphore.tryAcquire();
					if (result) {
						SocketIO db = this.availPool.poll();
						if (db == null || db.inUse) {
							System.out.println(db.getSocket().getInetAddress().getHostAddress()
									+ " is in use");
						}
						db.inUse = true;
						return db;
					}
					
					if (this.initConn * 2 < LIMIT) {
						System.out.println(String.format("double connection %d %d %d", this.initConn, this.initConn * 2, this.availPool.size()));
						this.initConn *= 2;
					}
					
					for (int i = 0; i < this.initConn / 2; i++) {
						SocketIO socket = null;
						while (socket == null) {
							try {
								socket = createSocket(server);
							}  catch (Exception e) {
								System.out.println(String.format("ip %s %s", this.server, e.getMessage()));
							}
						}
						this.availPool.add(socket);
						this.poolSemaphore.release();
					}
				}
				System.out.println("stuck on sockets!");
				poolSemaphore.acquire();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		SocketIO db = this.availPool.poll();
		if (db == null || db.inUse) {
			System.out.println(db.getSocket().getInetAddress().getHostAddress()
					+ " is in use");
		}
		db.inUse = true;
		return db;
	}

	public void checkIn(SocketIO db) {
		db.inUse = false;
		this.availPool.add(db);
		poolSemaphore.release();
	}
}
