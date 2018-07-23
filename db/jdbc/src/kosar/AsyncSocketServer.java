package kosar;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import kosar.SockIOPool;
import kosar.TokenObject;
import edu.usc.bg.server.BGServer;

/**
 * Copied from BG benchmark
 * 
 * @author Yazeed
 * 
 */
public class AsyncSocketServer extends Thread {
	public static ConcurrentHashMap<String, SockIOPool> SocketPool;

	public static ConcurrentHashMap<Integer, RequestHandler> handlers = new ConcurrentHashMap<Integer, RequestHandler>();

	static AtomicInteger handlercount = new AtomicInteger(0);

	public static Random random = new Random();
	public static boolean ServerWorking = true;

	public static TokenWorker[] tokenWorkers;
	public static AtomicInteger NumConnections = new AtomicInteger(0);

	public static ConcurrentHashMap<Integer, Semaphore> requestsSemaphores = new ConcurrentHashMap<Integer, Semaphore>();

	public static int NumSemaphores = 101;
	public static int NumThreadsPerSemaphore = 20;
	public static int NumWorkerThreads = (NumSemaphores * NumThreadsPerSemaphore);
	public static int ID;

	public static ConcurrentHashMap<Integer, ConcurrentLinkedQueue<TokenObject>> requestsQueue = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<TokenObject>>(
			NumSemaphores, .75f, 1);

	public static boolean verbose = false;
	public static boolean logFile = false;

	public static int port;
	private static int numSocketsPerClient = 30;

	public static ServerSocket server = null;
	static AtomicInteger startedWorker = new AtomicInteger();
	
	private Semaphore latch;
	
	private Properties properties;

	public void setup(int numSemaphores, int numThreadsPerSemaphore,
			int numSoc, int portNumber) {
		NumSemaphores = numSemaphores;
		NumThreadsPerSemaphore = numThreadsPerSemaphore;
		NumWorkerThreads = (NumSemaphores * NumThreadsPerSemaphore);
		numSocketsPerClient = numSoc;
		port = portNumber;
		SocketPool = new ConcurrentHashMap<String, SockIOPool>(0, 0.75f, 101);
		tokenWorkers = new TokenWorker[NumWorkerThreads];
	}

	/**
	 * @param numSemaphores
	 * @param numThreadsPerSemaphore
	 * @param numSoc
	 * @param portNumber
	 * @param properties 
	 */
	public AsyncSocketServer(int numSemaphores, int numThreadsPerSemaphore,
			int numSoc, int portNumber, Semaphore latch, Properties properties) {
		setup(numSemaphores, numThreadsPerSemaphore, numSoc, portNumber);
		int workerCount = 0;
		for (int i = 0; i < NumSemaphores; i++) {
			Semaphore semaphore = new Semaphore(0, true);
			requestsSemaphores.put(i, semaphore);
			requestsQueue.put(i, new ConcurrentLinkedQueue<TokenObject>());
			for (int j = 0; j < NumThreadsPerSemaphore; j++) {
				tokenWorkers[workerCount] = new DoDMLTokenWorker(i);
				workerCount++;
			}
		}

		for (int i = 0; i < NumWorkerThreads; i++) {
			tokenWorkers[i].start();
		}

		// wait until all worker is standby
		while (AsyncSocketServer.startedWorker.get() < NumWorkerThreads)
			;
		this.latch = latch;
		
		this.properties = properties;

		start();
	}

	public static void generateSocketPool(String remoteHostAddr) {
		System.out.println("generating socket pool " + remoteHostAddr);
		synchronized (SocketPool) {
			if (!SocketPool.containsKey(remoteHostAddr)) {
				SockIOPool pool = new SockIOPool(remoteHostAddr, numSocketsPerClient);
				SocketPool.put(remoteHostAddr, pool);
			}
		}
	}

	public void run() {
		AtomicInteger handlerID = new AtomicInteger(0);
		Socket socket = null;
		try {
			boolean flag=false;
			while(true){
			try {
				server = new ServerSocket();
				server.setReuseAddress(true);
                server.bind(new InetSocketAddress(port),10000);
				System.out.println("KOSAR is listening on port: " + server.getLocalPort());
				break;
			} catch (Exception e) {
				System.out.println("Error in Core Client port " + e.getMessage());
				
				if (flag){
				int i=BGServer.killBindingProcesses(port);
				if (i==0){
					if (!server.isClosed()){
					server.close();
					}
					server=null;
				}
				
			
				}
				flag=true;
				Thread.sleep(60000);
			}
			}
		
			System.out.println("the server is started");
			latch.release();
			while (AsyncSocketServer.ServerWorking) {
				try {
					// wait for a connection from a client
					socket = server.accept();
					RequestHandler handler = new RequestHandler(socket,
							handlerID.getAndIncrement(), this.properties);
					handler.setName("Request Handler " + handlerID);
					handlers.put(NumConnections.getAndIncrement(), handler);
					handler.start();
				} catch (Exception e) {
					System.out
							.println("Async Server Component is not able to listen and accept requests "
									+ e.getMessage());
					System.exit(1);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != server && !server.isClosed())
					server.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Finish stopping BG Server");
	}

	public static void shutdown() {

		System.out.println("shutdown ======");
		// if (CoreClient.clientId == 1) {
		// // only client 1 will send reinitialize command to Core.
		// try {
		// SocketIO socket = SocketPool.get(0).getConnection();
		// socket.writeBytes(PacketFactory.createCommandIdResposnePacket(PacketFactory.CMD_REINIT).array());
		// } catch (ConnectException e1) {
		// e1.printStackTrace();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// }
		ClientAction.log();

		ServerWorking = false;

		for (int rh : handlers.keySet()) {
			handlers.get(rh).stopHandling();
			try {
				handlers.get(rh).socket.closeAll();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			CoreClient.renewer.stop();
		} catch (Exception e) {
			
		}

		try {
			server.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void stopWorkers() {

		ServerWorking = false;
		for (int i = 0; i < NumSemaphores; i++)
			for (int j = 0; j < NumThreadsPerSemaphore; j++)
				// requestsSemaphores[i].release();
				requestsSemaphores.get(i).release();
		long totalNumberReqs = 0;
		for (int k = 0; k < NumWorkerThreads; k++) {
			try {

				totalNumberReqs = totalNumberReqs
						+ tokenWorkers[k].getNumRequestsProcessed();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			try {
				tokenWorkers[k].join();

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		System.out.println("Number of requests processed by workers:"
				+ totalNumberReqs + System.getProperty("line.separator")
				+ "All done with workers");
	}

	public static int getIndexForWorkToDo() {
		if (NumSemaphores == 1) {
			return 0;
		}
		return Math.abs(random.nextInt() % NumSemaphores);
	}

	public static void submitJob(TokenObject token) {
		int index = getIndexForWorkToDo();
		requestsQueue.get(index).add(token);
		requestsSemaphores.get(index).release();
	}

	public int getNumSocketsPerClient() {
		return numSocketsPerClient;
	}

}
