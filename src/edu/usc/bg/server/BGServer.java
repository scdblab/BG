package edu.usc.bg.server;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import edu.usc.bg.base.Client;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.DBFactory;
import edu.usc.bg.base.UnknownDBException;

public class BGServer extends Thread {
	private static ServerSocket server = null;
	// public static ConcurrentHashMap<Integer, SocketIO> sockets= new
	// ConcurrentHashMap<Integer, SocketIO>();
	public static ConcurrentHashMap<Integer, SockIOPool> SockPoolMapWorkload;
	// public static ConcurrentHashMap<Integer, SockIOPool> SockPoolMapWorkers;

	// public static Vector<RequestHandler> handlers = new
	// Vector<RequestHandler>();
	public static ConcurrentHashMap<Integer, RequestHandler> handlers = new ConcurrentHashMap<Integer, RequestHandler>();
	// public static RequestHandler[] handlers;
	static AtomicInteger handlercount = new AtomicInteger(0);
	private int CLIENT_CONNECT_PORT;
	public static int CLIENT_ID;
	// public static int NumSocketsTotal;
	// public static int NumSocketsWorkers;
	public static int NumSocketsWorkload;

	public static Random random = new Random();
	public static boolean ServerWorking = true;
	// public static Vector<TokenCacheWorker> tokenCacheWorkers = new
	// Vector<TokenCacheWorker>();
	public static TokenWorker[] tokenWorkers;
	public static AtomicInteger NumConnections = new AtomicInteger(0);
	// public static Vector<Semaphore> tokenCachedWorkToDo = new
	// Vector<Semaphore>();
	// public static Semaphore[] requestsSemaphores ;
	// public static Vector <Semaphore> requestsSemaphores= new
	// Vector<Semaphore>() ;
	public static ConcurrentHashMap<Integer, Semaphore> requestsSemaphores = new ConcurrentHashMap<Integer, Semaphore>();

	// public static int NumShutdownReqs;
	public static ConcurrentHashMap<Integer, ClientInfo> ClientInfo = new ConcurrentHashMap<Integer, ClientInfo>();
	public static int NumOfMembers;
	public static int NumOfThreads;
	public static int NumOfClients;
	public static int duration = 0; // in minutes
	public static int NumSemaphores = 101;// 31;//331;
	public static int NumThreadsPerSemaphore = 50;
	public static int NumWorkerThreads = (NumSemaphores * NumThreadsPerSemaphore);
	public static SemaphoreMonitorThread[] semaphoreMonitorThreads = new SemaphoreMonitorThread[NumSemaphores];
	// static Vector<ConcurrentLinkedQueue<Object>> requestsToProcess = new
	// Vector<ConcurrentLinkedQueue<Object>>();
	static ConcurrentHashMap<Integer, ConcurrentLinkedQueue<TokenObject>> requestsToProcess = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<TokenObject>>(
			NumSemaphores, .75f, 1);

	public static String LogFileName;
	public static boolean verbose = false;
	public static boolean logFile = false;

	BGServer(int clientId, int noclients, int nomembers, int nothreads,
			int port, int d, int numSoc, int testcase) { // this constructor is
															// used for testing

		duration = d;
		NumOfThreads = nothreads;
		// NumShutdownReqs=0;

		NumOfClients = noclients;
		NumOfMembers = nomembers;
		CLIENT_ID = clientId;
		// ReqId=1;
		CLIENT_CONNECT_PORT = port;
		LogFileName = "Client ID " + BGServer.CLIENT_ID + " Log File.txt";
		// NumSocketsWorkers=numSoc-1;
		NumSocketsWorkload = numSoc - 1;
		// NumSocketsTotal=numSoc-1;
		// NumWorkerThreads = 1;
		// handlers=new RequestHandler[(NumOfClients-1)*(NumSocketsTotal+1)];

		// try
		// {
		// FileWriter LogFileWriter = new FileWriter(LogFileName);
		// }
		// catch (Exception ex){
		// System.out.println("Error: Not creating log file");}
		// YAZ

		int workerCount = 0;

		start();
	}

	public BGServer(int clientId, int noclients, int nomembers, int nothreads,
			ConcurrentHashMap<Integer, ClientInfo> hm, int d, int numSoc) {
		// main constructor
		
		duration = d;
		NumOfThreads = nothreads;
		// NumShutdownReqs=0;
		ClientInfo = hm;
		NumOfClients = noclients;
		NumOfMembers = nomembers;
		CLIENT_ID = clientId;
		// ReqId=1;
		CLIENT_CONNECT_PORT = ClientInfo.get(CLIENT_ID).getPort();
		LogFileName = "Client ID " + BGServer.CLIENT_ID + " Log File.txt";
		// NumSocketsWorkers=numSoc;
		NumSocketsWorkload = numSoc;
		// NumSocketsTotal=NumSocketsWorkers+NumSocketsWorkload;
		SockPoolMapWorkload = new ConcurrentHashMap<Integer, SockIOPool>(
				NumOfClients - 1, 0.75f, 101);
		// SockPoolMapWorkers= new ConcurrentHashMap<Integer,
		// SockIOPool>(NumOfClients-1, 0.75f, 101);
		// NumWorkerThreads = 1;
		// handlers=new RequestHandler[(NumOfClients-1)*(NumSocketsTotal+1)];
		// requestsSemaphores=new Semaphore[NumSemaphores];
		tokenWorkers = new TokenWorker[NumWorkerThreads];
		// try
		// {
		// FileWriter LogFileWriter = new FileWriter(LogFileName);
		// }
		// catch (Exception ex){
		// System.out.println("Error: Not creating log file");}
		// YAZ

		int workerCount = 0;
		for (int i = 0; i < NumSemaphores; i++) {
			Semaphore semaphore = new Semaphore(0, true);
			// requestsSemaphores[i]=semaphore;
			// requestsSemaphores.add(semaphore);
			requestsSemaphores.put(i, semaphore);
			// semaphoreMonitorThreads[i]=new SemaphoreMonitorThread(i);

			requestsToProcess
					.put(i,
							(ConcurrentLinkedQueue<TokenObject>) new ConcurrentLinkedQueue<TokenObject>());

			// requestsToProcess.add((ConcurrentLinkedQueue<Object>)new
			// ConcurrentLinkedQueue<Object>());

			for (int j = 0; j < NumThreadsPerSemaphore; j++) {
				tokenWorkers[workerCount] = new TokenWorker(i, workerCount++);

			}
		}

		// start BGServer

		start();

		// Handshake

		// System.out.println("Handshake start");
		if (NumOfClients > 1) {
			boolean flag = true;
			while (NumConnections.get() < CLIENT_ID * (NumSocketsWorkload + 1)) {
				if (verbose)
					System.out.println("Num of Connections= " + NumConnections);
				if (flag) {
					flag = false;

					System.out.println("Waiting for other clients to connect");
				}

			}
			if (verbose)
				System.out.println("Time to handshake and generate sockets");

			for (int i = 0; i < NumOfClients; i++)
				if (i != CLIENT_ID)
					handshake(i);
			System.out.println("Handshake complete");
		}

		// Genrate Socket Pool
		generateSocketPool();

		for (int i = 0; i < NumWorkerThreads; i++) {
			// new Thread(tokenCacheWorkers[i], "TokenCacheWorker" + i).start();
			tokenWorkers[i].start();

		}

		boolean f = false;
		while (NumConnections.get() < (NumOfClients - 1)
				* (NumSocketsWorkload + 1)) {
			try {
				if (!f) {
					System.out.println("Waiting for all clients to connect...");
					f = true;
				}
				sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(System.out);
			}

		}

	}

	public static void initializeWorkers(String dbname, Properties properties) {
		// System.out.println("Registering db instance into workers");
		String value = properties.getProperty(Client.INSERT_IMAGE_PROPERTY);
		boolean insertImage = false;

		if (value != null) {
			insertImage = Boolean.parseBoolean(value);
		}
		try {
			if (Client.BENCHMARKING_MODE == Client.DELEGATE) {
				TokenWorker.dbPool = new DBPool(dbname, properties,
						TokenWorker.numDBConnections);

			}
			for (TokenWorker tokenWorker : tokenWorkers) {

				// DB db=null;
				// if (Client.BENCHMARKING_MODE==Client.DELEGATE)
				// {
				// db = DBFactory.newDB(dbname, properties);
				// db.init();
				// }

				// tokenWorker.setDB(db);
				tokenWorker.setInsertImgProperty(insertImage);
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}

	}

	BGServer(int semaphores, int workers, int numHandlers, int testcase) {
		// This constructor is used for testing test case 2.1

		duration = 0;
		NumOfThreads = 1;
		// NumShutdownReqs=0;

		NumOfClients = 1;

		CLIENT_ID = 0;
		// ReqId=1;
		// CLIENT_CONNECT_PORT=ClientInfo.get(CLIENT_ID).Port;
		LogFileName = "Client ID " + BGServer.CLIENT_ID + " Log File.txt";
		// NumSocketsWorkers=0;
		NumSocketsWorkload = 0;
		// NumSocketsTotal=numHandlers-1;
		// NumWorkerThreads = 1;
		// handlers=new RequestHandler[numHandlers];
		// handlers=new RequestHandler[(NumOfClients-1)*(NumSocketsTotal+1)];
		NumSemaphores = semaphores;
		NumThreadsPerSemaphore = workers;
		NumWorkerThreads = (NumSemaphores * NumThreadsPerSemaphore);

		// requestsSemaphores=new Semaphore[NumSemaphores];
		tokenWorkers = new TokenWorker[NumWorkerThreads];
		try {
			FileWriter LogFileWriter = new FileWriter(LogFileName);
		} catch (Exception ex) {
			System.out.println("Error: Not creating log file");
		}
		// YAZ

		int workerCount = 0;
		for (int i = 0; i < NumSemaphores; i++) {
			Semaphore semaphore = new Semaphore(0, true);
			// requestsSemaphores[i]=semaphore;
			// requestsSemaphores.add(semaphore);
			requestsSemaphores.put(i, semaphore);

			requestsToProcess
					.put(i,
							(ConcurrentLinkedQueue<TokenObject>) new ConcurrentLinkedQueue<TokenObject>());

			// requestsToProcess.add((ConcurrentLinkedQueue<Object>)new
			// ConcurrentLinkedQueue<Object>());

			for (int j = 0; j < NumThreadsPerSemaphore; j++) {
				tokenWorkers[workerCount++] = new TokenWorker(i, workerCount);

			}
		}

		// start workerThreads

		// start();

		// Handshake

		// System.out.println("Handshake start");

		for (int i = 0; i < NumWorkerThreads; i++) {
			// new Thread(tokenCacheWorkers[i], "TokenCacheWorker" + i).start();
			tokenWorkers[i].start();

		}

	}

	BGServer(int clientId, int noclients, int nomembers, int nothreads,
			ConcurrentHashMap<Integer, ClientInfo> hm, int d, int numSoc,
			int testcase) {
		// This constructor is used for testing (test case 1.3)

		duration = d;
		NumOfThreads = nothreads;
		// NumShutdownReqs=0;
		ClientInfo = hm;
		NumOfClients = noclients;
		NumOfMembers = nomembers;
		CLIENT_ID = clientId;
		// ReqId=1;
		CLIENT_CONNECT_PORT = ClientInfo.get(CLIENT_ID).getPort();
		LogFileName = "Client ID " + BGServer.CLIENT_ID + " Log File.txt";
		// NumSocketsWorkers=0;
		NumSocketsWorkload = numSoc;
		// NumSocketsTotal=NumSocketsWorkers+NumSocketsWorkload;
		SockPoolMapWorkload = new ConcurrentHashMap<Integer, SockIOPool>(
				NumOfClients - 1, 0.75f, 101);
		// NumWorkerThreads = 1;
		// handlers=new RequestHandler[(NumOfClients-1)*(NumSocketsTotal+1)];

		try {
			FileWriter LogFileWriter = new FileWriter(LogFileName);
		} catch (Exception ex) {
			System.out.println("Error: Not creating log file");
		}
		// YAZ

		int workerCount = 0;

		// start workerThreads

		start();

		if (NumOfClients > 1) {
			boolean flag = true;
			while (NumConnections.get() < CLIENT_ID * (NumSocketsWorkload + 1)) {
				if (verbose)
					System.out.println("Num of Connections= " + NumConnections);
				if (flag) {
					flag = false;

					System.out.println("Waiting for other clients to connect");
				}

			}
			if (verbose)
				System.out.println("Time to handshake and generate sockets");

			for (int i = 0; i < NumOfClients; i++)
				if (i != CLIENT_ID)
					handshake(i);
			System.out.println("Handshake complete");
		}

		// Genrate Socket Pool
		generateSocketPool();

	}

	public void generateSocketPool() {
		// generate socket pool

		for (int j = 0; j < NumOfClients; j++) {
			if (j != this.CLIENT_ID) {
				try {
					String server = BGServer.ClientInfo.get(j).getIP() + ':'
							+ BGServer.ClientInfo.get(j).getPort();
					SockIOPool p1 = new SockIOPool(server,
							BGServer.NumSocketsWorkload);
					SockIOPool p2 = null;
					// if (BGServer.NumSocketsWorkers!=0)
					// {
					// p2= new SockIOPool(server,BGServer.NumSocketsWorkers);
					// BGServer.SockPoolMapWorkers.put(j, p2);
					// }
					BGServer.SockPoolMapWorkload.put(j, p1);

					// if (BGServer.verbose)
					System.out.println("Client " + CLIENT_ID
							+ " genrated a pool of " + p1.availPool.size()
							+ " sockets with client " + j);
					if (p1.availPool.size() != BGServer.NumSocketsWorkload) {
						System.out
								.println("Error: The number of generated sockets is not equal the specified number. Exiting ...");
						System.exit(1);
					}
					// if (BGServer.NumSocketsWorkers!=0)
					// {
					// System.out.println("Client "+CLIENT_ID+" genrated a Workers pool of "
					// +p2.availPool.size()+" sockets with client "+j);
					// }

				} catch (Exception ex) {
					System.out.println("Error in Pool Intialization "
							+ ex.getMessage());

				}
			}
		}
	}

	public void handshake(int i) {
		boolean flag = true;
		boolean error = true;

		while (flag == true) {

			try {
				Socket socket1 = new Socket(ClientInfo.get(i).getIP(),
						ClientInfo.get(i).getPort());
				SocketIO socket = new SocketIO(socket1);

				// ccc
				socket.sendValue(RequestHandler.HANDSHAKE_REQUEST);
				// socket.writeInt(0);

				// wait for response

				// byte[] request = socket.readBytes();

				int a = socket.readInt();

				if (a == 1) {
					System.out.println("Handshake request from Client "
							+ CLIENT_ID + " to Client " + i + " Completes");
					flag = false;
					socket.closeAll();
				}
			} catch (Exception ex) {
				if (error == true) {
					System.out.println("Trying to Connect to other Clients: ");
					error = false;
				}

			}
		}
	}

	public void run() {
		AtomicInteger handlerID = new AtomicInteger(0);
		Socket socket = null;
		try {
			if (NumOfClients > 1) {
				boolean flag=false;
				while (true) {
					try {

						setServerSocket(new ServerSocket(CLIENT_CONNECT_PORT));
						System.out.println(" Client " + CLIENT_ID
								+ " Listener thread active on port: "
								+ getServerSocket().getLocalPort());
						break;
						// UnitTest.serverSemaphore.release(); // for testing

					} catch (Exception e) {
						System.out.println("Error in Server port "
								+ e.getMessage());
						if (flag){
						killBindingProcesses(CLIENT_CONNECT_PORT);
						}
						flag=true;
						Thread.sleep(10000);

					}
				} //while

			}
			// for (int i=0; i< ((NumOfClients-1)*(NumSocketsTotal+1)); i++)
			while (BGServer.ServerWorking)
			{
				try {
					// wait for a connection from a client
					socket = getServerSocket().accept();

					// System.out.println("Handler " + handlerID);
					RequestHandler handler = new RequestHandler(socket,
							handlerID.getAndIncrement(), CLIENT_ID);
					handler.setName("Request Handler " + handlerID);
					// handlers.add(handler);
					handlers.put(NumConnections.getAndIncrement(), handler);
					handler.start();

					// System.out.println("Connection from Client " + id +
					// " Received.");
				} catch (Exception e) {
					System.out
							.println("BG Server Component is not able to listen and accept requests "
									+ e.getMessage());
					System.exit(1);
				}
			} // end while

			// System.out.println("Server Finish");
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (null != getServerSocket() && !getServerSocket().isClosed())
					getServerSocket().close();
			} catch (IOException e) {
				e.printStackTrace(System.out);
			}
		}
		// for (int i=0; i< handlers.size();i++)
		// try{
		// handlers.get(i).join();
		// }
		//
		// catch (Exception ex){}
		System.out.println("Finish stopping BG Server");

	}

	public static int killBindingProcesses(int port) {
		// TODO Auto-generated method stub
		String netstat = " netstat -ano";
		int numKilled=0;
		String mypid=ManagementFactory.getRuntimeMXBean().getName().substring(0,ManagementFactory.getRuntimeMXBean().getName().indexOf('@'));
		Process p = null;
		try {
			p = Runtime.getRuntime().exec(netstat);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line;
		try {
			String pattern= ":" + port;
			//String pattern1 = "0.0.0.0" + ":" + port;
			//String pattern2 = Inet4Address.getLocalHost().getHostAddress().toString()+ ":" + port;
			while ((line = reader.readLine()) != null) {
				String patternLine=line;
				String[] tokens1 = line.split("    ");
				if (tokens1.length > 2) {
					patternLine=tokens1[1];
				}
				if (patternLine.contains(pattern) ) {
					String[] tokens2 = line.split(" ");
					String pid = tokens2[tokens2.length - 1];
					System.out.println("Killing Process:" + pid);
					if (pid.equals(mypid)){
						System.out.println("Wrong: trying to kill my proces:"+pid);
						
					}
					else{
					Process p2 = Runtime.getRuntime().exec("taskkill /F /pid " + pid);
					numKilled++;
					}
				}
				

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return numKilled;

	}

	public static void main(String[] args) {
		String mypid=ManagementFactory.getRuntimeMXBean().getName().substring(0,ManagementFactory.getRuntimeMXBean().getName().indexOf('@'));
		System.out.println(mypid);
		String netstat = " netstat -ano";
		Process p = null;
		try {
			p = Runtime.getRuntime().exec(netstat);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line;
		try {
			String pattern1 = "0.0.0.0" + ":" + 10001;
			String pattern2 = Inet4Address.getLocalHost().getHostAddress()
					.toString()
					+ ":" + 10001;
			while ((line = reader.readLine()) != null) {
				String patternLine=line;
				String[] tokens1 = line.split("    ");
				if (tokens1.length > 2) {
					patternLine=tokens1[1];
				}
				if (patternLine.contains(pattern1) || patternLine.contains(pattern2)) {
					String[] tokens2 = line.split(" ");
					String pid = tokens2[tokens2.length - 1];
					System.out.println("Killing Process:" + pid);
				//	Process p2 = Runtime.getRuntime().exec(
						//	"taskkill /F /pid " + pid);
				}
				

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void shutdown() {
		ServerWorking = false;

		// create a socket to make sure BGServer is not waiting for connection

		try {
			if (BGServer.NumOfClients > 1) {
				SocketIO socket = new SocketIO(new Socket(BGServer.ClientInfo
						.get(BGServer.CLIENT_ID).getIP(), BGServer.ClientInfo
						.get(BGServer.CLIENT_ID).getPort()));
				socket.sendValue(RequestHandler.STOP_HANDLING_REQUEST);
				socket.closeAll();

			}
		} catch (UnknownHostException e) {
			System.out.println("Error in Creating socket for shutdown");
			e.printStackTrace(System.out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}

		stopWorkers();
		// for(RequestHandler rh : handlers) {
		// rh.continueHandling=false;
		//
		// }
		for (int rh : handlers.keySet()) {
			handlers.get(rh).continueHandling = false;

		}
		// System.out.println("Finish stopping all workers ");

		// this.stop();
	}

	public static void stopWorkers() {

		ServerWorking = false;
		for (int i = 0; i < NumSemaphores; i++)
			for (int j = 0; j < NumThreadsPerSemaphore; j++)
				// requestsSemaphores[i].release();
				requestsSemaphores.get(i).release();
		long totalNumberReqs = 0;
		double maxReqinQ = 0;
		double maxDiffWorkers_Q = 0;
		String numSockets = "";
		for (int k = 0; k < NumWorkerThreads; k++) {
			try {

				totalNumberReqs = totalNumberReqs
						+ tokenWorkers[k].getNumRequestsProcessed();
				maxReqinQ = Math.max(maxReqinQ,
						tokenWorkers[k].getMaxNumReqInQ());
				maxDiffWorkers_Q = Math.max(maxDiffWorkers_Q,
						tokenWorkers[k].getMaxDiffWorkers_Req());

				// tokenCacheWorkers[k].db.cleanup(false);
			} catch (Exception e1) {
				e1.printStackTrace(System.out);
			}
			try {
				tokenWorkers[k].join();

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(System.out);
			}

			// System.out.println("Done with worker"+k);

		}
		// for (int i=0; i<BGServer.SockPoolMapWorkload.size();i++)
		// {
		// if (SockPoolMapWorkload.contains(i))
		// {
		// numSockets=numSockets+";"+BGServer.SockPoolMapWorkload.get(i).getNumConn();
		// }
		// }
		System.out.println("Max Requests in Q:" + maxReqinQ);
		System.out
				.println("Max Difference between requests and workers waiting:"
						+ maxDiffWorkers_Q);
		System.out.println("Number of requests processed by workers:"
				+ totalNumberReqs);
		if (Client.BENCHMARKING_MODE == Client.DELEGATE)
			System.out.println("Max waiting for DB:"
					+ TokenWorker.dbPool.getMaxWaiting());
		// System.out.println("Number of Sockets:"+numSockets);

		System.out.println("All done with workers");
		if (TokenWorker.dbPool != null) {
			TokenWorker.dbPool.shutdownPool();

		}
	}

	public static int getIndexForWorkToDo() {
		return Math.abs(random.nextInt() % NumSemaphores);
	}

	public static ServerSocket getServerSocket() {
		return server;
	}

	public void setServerSocket(ServerSocket server) {
		this.server = server;
	}

}
