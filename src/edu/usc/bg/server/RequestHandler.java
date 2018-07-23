package edu.usc.bg.server;
import com.mitrallc.sql.KosarSoloDriver;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import kosar.AsyncSocketServer;
import kosar.CoreClient;

import edu.usc.bg.base.Client;
import edu.usc.bg.base.ClientDataStats;
import edu.usc.bg.validator.ValidationMainClass;
import edu.usc.bg.workloads.CoreWorkload;



/**
 * Each Client has one RequestHandler, which manages its requests.
 * Though each command may be a multi-threaded request, the request handler
 * is single-threaded. Requests may be processed on a different thread.
 
 */
public class RequestHandler extends Thread {
	
	static Properties props; // for validation
	static PrintWriter outpS;// for validation
	public static boolean testing=false; // for testing purposes
	final public static int HANDSHAKE_REQUEST=0;
	final public static int STOP_HANDLING_REQUEST=98;
	final public static int FULL_SHUTDOWN_REQUEST=9999;
	final public static int SHUTDOWN_SOCKET_POOL_REQUEST=999;
	final public static int KILL_REQUEST=23453;
	final public static int THREAD_DUMP_REQUEST=9992;
	
	int serverID;
	int handlerID;
	byte[] clientID = null;
	boolean continueHandling = true;
	SocketIO socket = null;
	Scanner scanner = null;
	String trigger = null;
	static boolean shutdown=false;
	static private AtomicInteger NumShutdownReqs = new AtomicInteger(0);
	private static Vector<Thread> threads;
	
	 public volatile boolean done = false;
	int index;

	RequestHandler(Socket sock, int handlerID,int serverID) {
		try {
			this.socket = new SocketIO(sock);
		} catch (IOException e) {
			System.out.println("Error: RequestHandler - Unable to establish SocketIO from socket");
			e.printStackTrace(System.out);
		} 
		this.serverID = serverID;
		
	}
	

	RequestHandler() {
		
		// this constuctor is used for testing
		testing =true;
		
	}

	public void run() {
		byte request[] = null;
		int command = -1;
		int length = -1;
		
		try {
			while (continueHandling) {
				try {
					if (socket != null || !socket.getSocket().isConnected()
							|| !socket.getSocket().isClosed()) {
						//Reads in the request from the Client.
						//Format is always 4-byte int command followed by byte array
						//whose format is determined by the command.
						
						request = socket.readBytes();
					} else {
						System.out
						.println("Error: RequestHandler - Socket is null.");
					}
				} catch (EOFException eof) {
					System.out.println("End of Stream. Good Bye.");
				} catch (Exception e) {
					System.out.println(e.getMessage()+" Client Stream Shutdown. IP:"+ socket.getSocket().getInetAddress());
					System.exit(0);
					break;
				}
				//pulls the command from the request byte array
				command = ByteBuffer.wrap(Arrays.copyOfRange(request, 0, 4)).getInt();
				
				
			     
				
				switch(command)
				{
				//handshake
				case HANDSHAKE_REQUEST:
					
				
					// Response
					
					try {
						socket.writeInt(1);
					   continueHandling = false;
		
					} catch (Exception e) {
						System.out.println("Error: Handshake Response");
					}
					
					
				
					break;
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
				case TokenWorker.TIME_COMMAND:
				case TokenWorker.UPDATE_STATE_COMMAND:
				case TokenWorker.ACQUIRE_COMMAND:
				case TokenWorker.RELEASE_COMMAND:
				case TokenWorker.DELEGATE_COMMAND:
				case TokenWorker.DELEGATE_2_COMMAND:
				case TokenWorker.LOG_COMMAND:
				case TokenWorker.DELEGATE_0_COMMAND:
					
					// send request to workers
					
					// Yaz: the next four lines put request on requestsToProcess Q, It uses  tokenCachedWorkToDo semaphore
					index = BGServer.getIndexForWorkToDo();
					//synchronized(BGServer.requestsToProcess) {
						BGServer.requestsToProcess.get(index).add(new TokenObject(socket, request));
//						BGServer.requestsSemaphores[index].release();
						BGServer.requestsSemaphores.get(index).release();

					//}
					
					break;
					
				case SHUTDOWN_SOCKET_POOL_REQUEST: //shutdown request from the coordinator to shutdown the socketPool
//				
					System.out.println("Received Socket Pool Shutdown Request");
					String numSockets="";
					for (int i=0; i< BGServer.NumOfClients;i++)
					{
						if (i!= BGServer.CLIENT_ID)
						{
							
							SockIOPool p= BGServer.SockPoolMapWorkload.get(i);
							
							if (p==null)
							{
								System.out.println("Pool Null");
							}
							else
							{
								numSockets=numSockets+":"+p.getNumConn();
								p.sendStopHandling(p.availPool,STOP_HANDLING_REQUEST);
								p.shutDown();
							}
						}
					}
					System.out.println("Number of Sockets:"+numSockets);
					
				
					
//					for (int i=0;i<BGServer.handlers.size();i++)
//					{
//						BGServer.handlers.get(i).socket.sendValue(98);
//						
//					}
//					this.continueHandling=true;
					break;
				case THREAD_DUMP_REQUEST:
					try{
					System.out.println("Receieved Dump Thread Request");
					System.out.println("Generating thread dump...");
					String dump=generateThreadDump();
					PrintWriter out = new PrintWriter("/home/hieun/Desktop/ratinglogs/tD-"+InetAddress.getLocalHost().getHostAddress()+"-"+System.currentTimeMillis()+".txt");
					System.out.println("Done generating dump file");
					out.println(dump);
					out.close();
					continueHandling=false;
					}
					catch (Exception e){
						e.printStackTrace(System.out);
					}
					break;
					
				case KILL_REQUEST:
					try{
					System.out.println("Receieved KILL Request");
					System.out.println(" Exiting...");
					BGServer.getServerSocket().close();
					}
					catch(Exception e){
						e.printStackTrace(System.out);
					}
					// kill my pid
					String mypid=ManagementFactory.getRuntimeMXBean().getName().substring(0,ManagementFactory.getRuntimeMXBean().getName().indexOf('@'));
					Process p2 = Runtime.getRuntime().exec("taskkill /F /pid " + mypid);
					System.exit(0);
					break;
				case FULL_SHUTDOWN_REQUEST: // full shutdown request from coordinator
					System.out.println("Received Full Shutdown Request");
					this.continueHandling=false;
					
					BGServer.shutdown();
					for (int i=0; i< BGServer.handlers.size();i++){
						try{
							if (BGServer.handlers.get(i).handlerID!=this.handlerID){
								if (!BGServer.handlers.get(i).socket.socket.isClosed()){
								
								BGServer.handlers.get(i).socket.closeAll();
								}
							BGServer.handlers.get(i).join();
							}
						}

					catch (Exception ex){ex.printStackTrace(System.out);}
					}
					String dbname="";
					try{
					dbname = props.getProperty(Client.DB_CLIENT_PROPERTY, Client.DB_CLIENT_PROPERTY_DEFAULT);
				
					System.out.println("Finish Stopping all handlers");

					
					// do validation
					if ( (CoreWorkload.updatesExist && CoreWorkload.readsExist) || ( !CoreWorkload.updatesExist && CoreWorkload.graphActionsExist )  ) {
						// threadid,listOfSeqs seen by that threadid
						ClientDataStats expStat = new ClientDataStats();
						HashMap<Integer, Integer>[] seqTracker = new HashMap[Client.threadCount+BGServer.NumWorkerThreads];
						HashMap<Integer, Integer>[] staleSeqTracker = new HashMap[Client.threadCount+BGServer.NumWorkerThreads];
						long dumpVTimeE = 0, dumpVTimeS = 0;

						System.out
						.println("--Discarding, dumping and validation starting.");
						dumpVTimeS = System.currentTimeMillis();

						ValidationMainClass.dumpFilesAndValidate(props, seqTracker,
								staleSeqTracker, expStat,outpS, props.getProperty(
										Client.LOG_DIR_PROPERTY, Client.LOG_DIR_PROPERTY_DEFAULT));
						dumpVTimeE = System.currentTimeMillis();
						System.out
						.println("******* Discrading, dumping and validation is done."
								+ (dumpVTimeE - dumpVTimeS));
						expStat.setDumpAndValidateTime((double) (dumpVTimeE - dumpVTimeS)); // the dumptime
					
					
					}
					if (CoreClient.enableCache && dbname.toLowerCase().contains("kosar")){
						AsyncSocketServer.shutdown();
					}

					if (dbname.contains("JdbcDBClient_KOSAR")) {
						System.out.println("Shutting down Core transparent client");
						KosarSoloDriver.closeSockets();
						}
					System.out.println("Exiting...");
					System.exit(0);
					}catch (Exception e){
						System.out.print("Error during full shutdown:");
						e.printStackTrace(System.out);
					}
					break;
					
				case STOP_HANDLING_REQUEST:   // stop handling
					continueHandling = false;
					break;
				
				case 99: // not used anymore. Shutdown is performed by coordinator
					continueHandling = false;
					NumShutdownReqs.incrementAndGet() ;

					//if (BGServer.verbose)
					synchronized (RequestHandler.class)
					{
					if ((( NumShutdownReqs.get()==(BGServer.NumOfClients-1)* (BGServer.NumSocketsWorkload))&& !shutdown))
					{
						shutdown=true;
								
						System.out.println("Client " + BGServer.CLIENT_ID +" is shutting down. Recieved " + NumShutdownReqs.get()+" shutdown requests" );
									
						BGServer.shutdown();
//						System.out.println(" Num updates: " + TokenWorker.NumUpdates.get());

						System.out.println("Closing workers pool");
						// shutdown workers socket pool
						
//							for (int i=0; i< BGServer.NumOfClients;i++)
//							{
//								if (i!= BGServer.CLIENT_ID)
//								{
//									SockIOPool p= BGServer.SockPoolMapWorkers.get(i);
//									if (p==null)
//									{
//										System.out.println("Pool Null");
//									}
//									else
//									{	
//										System.out.println("Start shutdown worker pool");
//										System.out.println("Worker pool: avilable soc: " + p.availPool.size()+ " busy: " + p.busyPool.size()+" dead " + p.deadPool.size() );
//										p.sendStopHandling(p.availPool,98);
//										p.shutDown();
//										System.out.println("Finish shutdown worker pool");
//									}	
//								}
//							}
						}
					}
					
					
					break;
					
				case 12:   // unit test case 1.2
					int requesttest = ByteBuffer.wrap(Arrays.copyOfRange(request, 4, 8)).getInt();
					if (requesttest==1)
					{
						socket.sendValue(1);
					}
					else
						System.out.println("Error Test Request");
					
					break;
					
				default:
					System.out
					.println("Error: RequestHandler - Could not recognize command "
							+ command);
					break;
				}
			    
				
			

			} }// end while
		catch (Exception e) {
			e.printStackTrace(System.out);
		} 
		finally {
			try {
				socket.closeAll();
			} catch (IOException e) {
				System.out.println("Error: RequestHandler - Failed to close streams");
				e.printStackTrace(System.out);
			}
			if (BGServer.verbose)
			System.out.println("RequestHandler " + " shut down. I/O cleanup complete.");
		}
	}

	
	


	
	public int sumShutdownReqs()
	{
		int sum=0;
		for (int i=0; i<BGServer.handlercount.get();i++)
		    if (BGServer.handlers.get(i).continueHandling==false)
		    	sum=sum+1;
		return sum;
	}
	
	
	public SocketIO getSocket() {
		return socket;
	}

	public void setSocket(SocketIO socket) {
		this.socket = socket;
	}

	public byte[] getClientID() {
		return clientID;
	}

	public void setClientID(byte[] clientID) {
		this.clientID = clientID;
	}


	public static void setValidationInfo(Properties props1, PrintWriter outpS1, Vector<Thread> threadsa) {
		// TODO Auto-generated method stub
		threads=threadsa;
		
		props=props1;
		outpS=outpS1;
		
	}

	 public static String generateThreadDump() {
	        final StringBuilder dump = new StringBuilder();
	        try{
	        for (Thread thread : threads) {
	            dump.append('"');
	            dump.append(thread.getName());
	            dump.append("\" ");
	            final Thread.State state = thread.getState();
	            dump.append("\n   java.lang.Thread.State: ");
	            dump.append(state);
	            final StackTraceElement[] stackTraceElements = thread.getStackTrace();
	            for (final StackTraceElement stackTraceElement : stackTraceElements) {
	                dump.append("\n        at ");
	                dump.append(stackTraceElement);
	            }
	            dump.append("\n\n");
	        }
	        }catch (Exception ex){
	        	ex.printStackTrace(System.out);
	        }
	        return dump.toString();
	    }

}