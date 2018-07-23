package edu.usc.bg.server;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;

import javax.management.ListenerNotFoundException;


public class BGServerListener {
	static StringBuffer logString=new StringBuffer();
	static BufferedWriter log =null;
	static boolean loadDbPartitionedBG=false;
	static String partitionedBGRunString=" java -Xmx15G -cp /home/hieun/Desktop/BG/bin:/home/hieun/Desktop/BG/lib/* edu.usc.bg.BGMainClass";
	static String messagePassingBGRunString=" java -Xmx15G -cp /home/hieun/Desktop/BG/bin:/home/hieun/Desktop/BG/lib/* edu.usc.bg.BGMainClass";
	static boolean logging=false;
	public static int Partitioned=1;
	public static int MsgPassing=2;
	public static int benchmarkMode=MsgPassing;
	public static boolean verbose=true;
	public static void main(String[] args) throws IOException {
		ServerSocket BGCoordListenerSocket = null;
		Socket coordConnection = null;
		InputStream inputstreamFromCoord = null;
		OutputStream outputstreamToCoord = null;
		PrintWriter printerToCoord = null;
		// scanner removes the space form the end of the token
		Scanner scannerFromCoord = null;
		Process _BGProcess = null;
		Socket requestSocketToBGClient = null;
		OutputStream outputstreamToBG = null;
		InputStream inputstreamFromBG = null;
		InputStream BGClientStdout = null;
		BufferedReader readerFromBGClientStdout = null;
		PrintWriter printerToBGSocket = null;
		Scanner scannerFromBGSocket = null;
		String dir="C:/Users/yaz/Documents/BG/BGServerListenerLogs";

		String arguments;
		HashMap<String, String> params = new HashMap<String, String>();
		boolean listenerRunning = true;


		if (args.length < 1) {
			System.out.println("The listener's config file is missing.");
			System.exit(0);
		}
		System.out.println("Killing any running BGClient ...");
		killBG();

		String configFile = args[0];

		// read the port, validation oracle details and the export file from
		// config file
		// read the parameters from the file
		readParams(configFile, params);
		int port = Integer.parseInt(params.get("port"));
		//	if (params.get("partitionedbg")!=null)
		//	partitionedBGRunString=params.get("partitionedbg");
		//	if (params.get("messagepassingbg")!=null)
		//	messagePassingBGRunString=params.get("messagepassingbg");


		///
		//listener code

		try {
			BGCoordListenerSocket = new ServerSocket(port, 10);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			System.out.println("Listener Error");
			e2.printStackTrace();
		}
		System.out
		.println("BGListener: started and Waiting for connection on "
				+ port);
		while (listenerRunning)
		{
			System.out.println("Listener is waiting for connection ...");
			File file = null;
			FileWriter fstream = null;



			try {



				coordConnection = BGCoordListenerSocket.accept();

				inputstreamFromCoord = coordConnection.getInputStream();
				outputstreamToCoord = coordConnection.getOutputStream();
				printerToCoord = new PrintWriter(new BufferedOutputStream(
						outputstreamToCoord));
				scannerFromCoord = new Scanner(inputstreamFromCoord);
				new KillThread(scannerFromCoord, inputstreamFromCoord).start();

				//read file


				System.out.println("BGListener: Connection received from "
						+ coordConnection.getInetAddress().getHostName());

				//		Thread.sleep(5000);
				String BGParams = "", line = "";

				//		printerToCoord.println("Connected ");
				//		printerToCoord.flush();

				arguments = scannerFromCoord.nextLine();
				System.out.println("Command Received: " +arguments);
				String machineid="";
				String bmode ="";
				String threadcount="";
				String enablelogging ="";
				String numClient="";
				String numMembers="";

				String tokens[]=arguments.split(" ");
				for (String t:tokens)
				{
					if (t.contains("nummembers"))
					{
						numMembers = t.substring(t.indexOf('=')+1);
					}
					if (t.contains("numclients"))
					{
						numClient = t.substring(t.indexOf('=')+1);
					}
					if (t.contains("enablelogging"))
					{
						enablelogging = t.substring(t.indexOf('=')+1);
					}
					if (t.contains("threadcount"))
					{
						threadcount = t.substring(t.indexOf('=')+1);
					}
					if (t.contains("machineid"))
					{
						machineid = t.substring(t.indexOf('=')+1);
					}
					if (t.contains("benchmarkingmode"))
					{
						bmode = t.substring(t.indexOf('=')+1);
						if(bmode.equalsIgnoreCase("retain")||bmode.equalsIgnoreCase("delegate"))
						{
							benchmarkMode=MsgPassing;
							System.out.println("Listener is running Message-Passing BG");

						}
						else
						{
							benchmarkMode=Partitioned;
							System.out.println("Listener is running Partitioned BG");


						}


					}
				}


				if (logging)
				{
					logString.delete(0, logString.length());
					file = new File(dir+"/listenerLog-"+ System.currentTimeMillis()+"-members"+numMembers+"-numclients"+numClient+"-mid"+machineid+"-mode"+bmode+"-thread"+threadcount+ "-log"+enablelogging+ ".txt");
					fstream = new FileWriter(file);
					log = new BufferedWriter(fstream);
					logString.delete(0, logString.length()) ;
				}

				if (logging)
				{
					logString.append("Port="+ port+ System.getProperty("line.separator")+ "arguments="+arguments+System.getProperty("line.separator"));
					//		if(testLog != null && testLog.length() != 0) {
					//			log.write(testLog.toString());
					//			log.flush();
					//		}
				}

				// arguments="-p numclients=2 -p clients=127.0.0.1:53159,127.0.0.1:53139  -p clientid=1 -p nummembers=1000 -p numrequests=1000 -p numthreads=1";
				Process _BGLoadProcess=null;
				if (benchmarkMode==Partitioned)
				{
					//			 System.out.println("Listener is running current BG");

					if (loadDbPartitionedBG)
					{
						String dbLoadMSg=createDbLOadMsg(arguments);
						System.out.println("DB Load Args: "+dbLoadMSg);
						Process _DBLoadProcess = Runtime.getRuntime ().exec(partitionedBGRunString + " "+dbLoadMSg  );
						InputStream dstdout = _DBLoadProcess.getInputStream ();
						BufferedReader dreader = new BufferedReader (new InputStreamReader(dstdout));				
						String dline ="";

						while ((dline = dreader.readLine ()) != null ) 
						{


							System.out.println ("Stdout: "+ dline);
						}

						_DBLoadProcess.waitFor();
					}

					System.out.println("Done loading DB");
					printerToCoord.println("Done loading DB");
					printerToCoord.flush();
					String msg="";
					boolean run=false;
					while( scannerFromCoord.hasNextLine())
					{
						msg=scannerFromCoord.nextLine();
						if (msg.equalsIgnoreCase("Run Benchmark"))
						{
							run=true;
							break;
						}

					}
					if (!run)
						continue;
					_BGLoadProcess = Runtime.getRuntime ().exec(partitionedBGRunString + " "+arguments  );


				}
				else
				{
					// System.out.println("Listener is running new BG");
					System.out.println("Done loading DB");
					printerToCoord.println("Done loading DB");
					printerToCoord.flush();
					String msg="";
					boolean run=false;
					while( scannerFromCoord.hasNextLine())
					{
						msg=scannerFromCoord.nextLine();
						if (msg.equalsIgnoreCase("Run Benchmark"))
						{
							run =true;
							break;
						}

					}
					if (!run)
						continue;
					_BGLoadProcess = Runtime.getRuntime ().exec(messagePassingBGRunString + " "+arguments  );
					System.out.println("Execute"+messagePassingBGRunString + " " + arguments );

				}
				//		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {  
				//		    public void run() {  
				//		    	_BGLoadProcess.destroy();  
				//		        }  
				//		})); 
				//	

				InputStream stdout = _BGLoadProcess.getInputStream ();
				BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));				
				String BGline ="";
				int count=0;
				String stats="";
				boolean sendCoord=false;
				OSStatThread osThread=null;
				while ((BGline = reader.readLine ()) != null ) 
				{		

					System.out.println ("Stdout: "+ BGline);
					if (BGline.equalsIgnoreCase("Close server socks")){
						_BGLoadProcess.destroy();
						break;
					}
					if (logging)
					{
						//			testLog.delete(0, testLog.length());
						logString.append(BGline+System.getProperty("line.separator"));
						//			if(testLog != null && testLog.length() != 0) {
						//				log.write(testLog.toString());
						//				log.flush();
						//			}
					}
					if (BGline.contains(("Starting benchmark")))
					{
						osThread = new OSStatThread(printerToCoord);
						osThread.start();
					}

					if (BGline.contains(("Maximum time elapsed")))
					{

						osThread.setEnd();
					}
					else if (BGline.contains(("SHUTDOWN!!!")))
					{
						osThread.setEnd();
						printerToCoord.println(BGline);
						printerToCoord.flush();
						sendCoord=false;
					}

					if (BGline.contains("stale")||BGline.contains("Number of requests processed by workers") || BGline.contains("Max Requests in Q")||BGline.contains("Max Difference between requests and workers waiting")|| BGline.contains("Max waiting for DB")|| BGline.contains("Number of Sockets") || BGline.contains("local actions"))
					{
						stats=stats+System.getProperty("line.separator")+BGline;


					}


					if (sendCoord)
					{
						stats=stats+System.getProperty("line.separator")+BGline;
					}
					if (BGline.equals("DONE"))
						sendCoord=true;
				}
				osThread.setEnd();

				printerToCoord.println(stats);
				printerToCoord.flush();


				//wait for the BG to complete

				_BGLoadProcess.waitFor();

				if (logging)
				{
					log.write(logString.toString());
					log.flush();

				}









			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			//close connection to coord

			finally
			{

				if (inputstreamFromCoord != null)
					inputstreamFromCoord.close();
				if (outputstreamToCoord != null)
					outputstreamToCoord.close();
				if (printerToCoord != null)
					printerToCoord.close();
				if (scannerFromCoord != null)
					scannerFromCoord.close();


				if (log != null) {

					log.close();
				}
				try{
					if(!KillThread.soc.isClosed())
						KillThread.soc.close();
				}
				catch(Exception ex){
					System.out.println("Error:BG Server Listener Kill Thread: "+ex.getMessage());
					ex.printStackTrace();
				}
			}
			System.out.println("Listener has finished");

		} // end while

	}

	private static String createDbLOadMsg(String arguments) {
		// TODO Auto-generated method stub
		String [] args= arguments.split(" ");
		int loadthreads=0;
		int threadcountIndex=-1;
		for (int i=0;i<args.length;i++)
		{
			if (args[i].equalsIgnoreCase("-t"))
			{
				args[i]="-loadindex";
			}
			else if (args[i].equalsIgnoreCase("-s"))
			{
				args[i]="";
			}
			else if (args[i].contains("numloadthreads"))
			{
				loadthreads=Integer.parseInt(args[i].substring(args[i].indexOf('=')+1, args[i].length()));
			}
			else if (args[i].contains("threadcount"))
			{
				threadcountIndex=i;
			}

		}
		args[threadcountIndex]="threadcount="+loadthreads;
		String loadDb="";
		for (int i=0;i<args.length;i++)
		{
			loadDb=loadDb+args[i]+" ";
		}
		return loadDb;

	}


	public static void killBG() {
		// TODO Auto-generated method stub
		String TASKLIST = "tasklist";
		Process p=null;
		try {
			p = Runtime.getRuntime().exec("jps");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				p.getInputStream()));
		String line;
		try {
			while ((line = reader.readLine()) != null) {

				if (line.contains("BGMainClass"))
				{ 
					String pid=line.substring(0,line.indexOf(' '));
					Process p2=Runtime.getRuntime().exec("taskkill /F /pid " +pid);

				}

			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	public static void readParams(String fileName,
			HashMap<String, String> params) {
		DataInputStream in = null;
		BufferedReader br = null;
		try {
			FileInputStream fstream = new FileInputStream(fileName);
			in = new DataInputStream(fstream);
			br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while ((strLine = br.readLine()) != null) {
				if (strLine.trim() == "")
					continue;
				else {
					params.put((strLine.substring(0, strLine.indexOf("=")).toLowerCase()),
							strLine.substring(strLine.indexOf("=") + 1));
				}
			}
		} catch (Exception e) {// Catch exception if any
			System.out.println("Error: " + e.getMessage());
		} finally {
			try {
				if (in != null)
					in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
class KillThread extends Thread{
	int port=11111;
	boolean run=true;
	public static ServerSocket soc;
	Scanner coordScanner;
	InputStream inputstreamCoord;
	public KillThread(Scanner scannerFromCoord) {
		// TODO Auto-generated constructor stub
		coordScanner=scannerFromCoord;

	}
	public KillThread(Scanner scannerFromCoord, InputStream inputstreamFromCoord) {
		// TODO Auto-generated constructor stub
		coordScanner=scannerFromCoord;
		inputstreamCoord=inputstreamFromCoord;
	}
	public void run(){


		try {
			soc = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}






		try {
			soc.accept();
		} catch (IOException e) {
			// TODO Auto-generated catch block

		}
		try {
			soc.close();
			BGServerListener.killBG();				
			coordScanner.close();
			inputstreamCoord.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block

		}




	}
}


class OSStatThread extends Thread {
	String cmd = "typeperf \"\\Memory\\Available bytes\" \"\\processor(_total)\\% processor time\" \"\\PhysicalDisk(_Total)\\Avg. Disk Write Queue Length\" \"\\Network Interface(*)\\Bytes Total/sec\" -sc 1";
	boolean end = false;
	double availableMem = 0.0;
	double cpuTime = 0.0;
	double avgQLength = 0.0;
	double netwrokBytesPerSec = 0.0;
	PrintWriter _outPrint = null;

	public OSStatThread(PrintWriter outPrint) {
		_outPrint = outPrint;
	}

	public void setEnd() {
		end = true;
	}

	public void run() {
		while (!end) {
			try {
				int cnt = 0;
				Process osStats = Runtime.getRuntime().exec(cmd);
				InputStream stdout = osStats.getInputStream();
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(stdout));
				String osline = "";
				while ((osline = reader.readLine()) != null) {

					cnt++;

					// parse the line and send to coord
					if (cnt == 3) {
						//						 System.out.println ("OSS: "+ osline);
						String memory,cpu,network,disk;
						String[] stats = osline.split(",");
						double mem=Double.parseDouble(stats[1].substring(stats[1].indexOf("\"") + 1,stats[1].lastIndexOf("\"")));
						mem=mem/(1024*1024);
						memory="Available MEM(MB):"+ mem + ",";
						//						_outPrint.print(memory);
						//						_outPrint.flush();
						cpu="CPU:"
								+ stats[2].substring(
										stats[2].indexOf("\"") + 1,
										stats[2].lastIndexOf("\"")) + ",";
						//						_outPrint.print(cpu);
						//						_outPrint.flush();
						disk="DISK:"
								+ stats[3].substring(
										stats[3].indexOf("\"") + 1,
										stats[3].lastIndexOf("\"")) + ",";
						//						_outPrint.print(disk);
						//						_outPrint.flush();
						double net=Double.parseDouble(stats[4].substring(stats[4].indexOf("\"") + 1,stats[4].lastIndexOf("\"")));
						net=net/(1024*1024);
						network="NTBW(MB/sec):"
								+ net + ",";
						//						_outPrint.print(network);
						//						_outPrint.flush();
						String line="OS="+cpu+memory+network+disk;
						System.out.println(line);
						if (BGServerListener.logging)
						{
							//						testLog.delete(0, testLog.length());
							BGServerListener.logString.append(line+System.getProperty("line.separator"));
							//						if(testLog != null && testLog.length() != 0) {
							//							BGServerListener.log.write(testLog.toString());
							//							BGServerListener.log.flush();
							//						}
						}
					}
				}

				osStats.waitFor();
				if (osStats != null)
					osStats.destroy();
				Thread.sleep(10000);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}

