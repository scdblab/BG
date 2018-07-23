package edu.usc.bg;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;


import edu.usc.bg.base.Client;
import edu.usc.bg.workloads.CoreWorkload;

public class BGMainClass {
	public static final int ONETIME = 0;
	public static final int REPEATED = 1;
	public static final int COMMANDLINE = 2;
	public static final String ONETIMESTRING = "onetime";
	public static final String REPEATEDSTRING = "repeated";
	public static final String COMMANDLINESTRING = "commandline";
	public static int exeMode = ONETIME;
	public static String SHUTDOWN_TOKEN = "shutdown";
	public static int port = 54546;

	public static void main(String[] args) {
		// BGMailClass single -schema -db ...
		// BGMainClass continual port
		if (args.length < 1) {
			System.out.println("Err: Missing execution mode!");
			System.exit(0);
		}
		
		Client BGClient = new Client();
		if (args[0].equalsIgnoreCase(REPEATEDSTRING))
			exeMode = REPEATED;
		else if (args[0].equalsIgnoreCase(COMMANDLINESTRING))
			exeMode=COMMANDLINE;
		else
			exeMode = ONETIME;

		//running it as standalone app which will die after execution
		if (exeMode == ONETIME) {
			System.out.println("BGClient is in "+ONETIMESTRING+" mode.");
			// remove the first argument and pass the whole args to the
			args = removeElements(args, 0);
			BGClient.runBG(args, null);
//			System.exit(0);
			
		} 
		else if (exeMode==COMMANDLINE)
		{
			System.out.println("BGClient is in "+COMMANDLINESTRING+" mode.");
			// remove the first argument and pass the whole args to the
			
			args = removeElements(args, 0);
			CoreWorkload.commandLineMode=true;
			BGClient.runBG(args, null);
			
		}
		else if (exeMode == REPEATED) {
			ServerSocket BGSocket;
			Socket connection = null;
			boolean shutdown = false;
			// there should be a port to start listening on for commands from
			// the listener
			checkPortAvailability(args);

			try {
				BGSocket = initConnection(args);
		
				while (!shutdown) {
					connection = getConncetion(BGSocket);
					InputStream in = connection.getInputStream();
					//read the message passed
					String BGArgs = readFromInputStream(in);
					
					if(BGArgs.contains(SHUTDOWN_TOKEN)){
						shutdown = true;
						continue;
					}
					//send all the args to the client class 
					args = removeElements(BGArgs.split(" "), REPEATEDSTRING);
					BGClient.runBG(args, connection);
				}
				System.out.println("BGClient is shutting down!Bye!");

			} catch (IOException e) {
				e.printStackTrace(System.out);
			} finally {
				try {
					if (connection != null)
						connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}
	}

	private static String readFromInputStream(InputStream in){
		Scanner inScan = new Scanner(in);
		String BGArgs = "", line=""; 
		while(inScan.hasNext()){
			line = inScan.next();
			if(!line.equals("#")){
				line+=" ";
				BGArgs += line;
			}else
				break;
		}
		return BGArgs;
	}
	/**
	 * @param BGSocket
	 * return
	 * @throws IOException
	 */
	private static Socket getConncetion(ServerSocket BGSocket)
			throws IOException {
		Socket connection;
		System.out.println("BGClient is in "+REPEATEDSTRING+" mode.");
		connection = BGSocket.accept();
		System.out.println("BGClient: Connection received from "
				+ connection.getInetAddress().getHostName());
		return connection;
	}


	/**
	 * @param args
	 * return
	 * @throws IOException
	 */
	private static ServerSocket initConnection(String[] args)
			throws IOException {
		ServerSocket BGSocket;
		port = Integer.parseInt(args[1]);
		System.out.println("Started");
		BGSocket = new ServerSocket(port, 10);
		System.out.println("BGClient: started and Waiting for connection on "
				+ port);
		return BGSocket;
	}


	/**
	 * @param args
	 */
	private static void checkPortAvailability(String[] args) {
		if (args.length < 2) {
			System.out
			.println("You need to specify a port for the communication between the BGClient and the BGListener");
			System.exit(0);
		}
	}
	
	
	public static String[] removeElements(String[] args, int idx) {
	    String [] result = new String[args.length-1];
	    int cnt =0;
	    for(int i=0; i<args.length; i++){
	    	if( i == idx)
	    		continue;
	    	result[cnt]= args[i];
	    	cnt++;
	    }
	    return result;
	}
	
	public static String[] removeElements(String[] args, String token) {
	    ArrayList<String> result = new ArrayList<String>();
	    int cnt = args.length;
	    boolean tokenFound = false;
	    int i =-1;
	    while(i<cnt-1){
	    	i++;
	    	if( args[i].equals(token)){
	    		tokenFound = true;
	    		continue;
	    	}
	    	if(tokenFound){
	    		result.add(args[i]);	    		
	    	}
	    }
	    
	    String[] parsedArgs = new String[result.size()];
	    result.toArray(parsedArgs);
	    return parsedArgs;
	}

}
