package edu.usc.bg.server;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;


public class UnitTest extends Thread {

	/**
	 * @param args
	 */
	public static InputParsing a;
	public static Semaphore serverSemaphore= new Semaphore(0,true);
	public static SockIOPool p1;
	public static int NumThreads=10;
	public static int NumSockets=20;
	public static int NumIterations=50;
	public static int testCase; 
	public static int repeat=100;
	public static int numWorkerThreads;
	public static int numSemaphores;
	public AtomicInteger msgCount=new AtomicInteger();
	public static int numRequests;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime=System.currentTimeMillis();
  
		//switch
//		testCase2_1();
		long EndTime=System.currentTimeMillis();
		System.out.println("Duration= " + (EndTime-startTime)+ " msec" );
		
		// testCase1_2();
		//testCase1_3(args);
//		testCase2_1();
		testCase2_2();

	}





	UnitTest()
	{



	}
	public void run()
	{
		if (testCase==11)
		{
			for (int i=0; i<NumIterations;i++)
			{
				SocketIO s1=null;
				try {
					s1= p1.getConnection();
					sleep(100);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println("Pool connection Error");
				}
				p1.checkIn(s1);
			}
			}
		///////////////////////////////////////////////////

		else if(testCase==12)
		{
			SocketIO s1=null;
			for (int i=0; i<repeat;i++)
			{
				try {

					s1= p1.getConnection();

					ByteBuffer bb = ByteBuffer.allocate(8);
					bb.putInt(12);
					bb.putInt(1);
					s1.writeBytes(bb.array());

					byte[] request = s1.readBytes();
					int r=ByteBuffer.wrap(request).getInt();
					if (r==1)
						msgCount.incrementAndGet();
					else
						System.out.println("Error recieved ");
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println("Pool connection Error");
				}
				p1.checkIn(s1);
			}
		}
		////////////////////////////////////////////////////////////

		else if(testCase==13)
		{
			for (int i1=0; i1<a.NumClients;i1++)
			{
				if (i1!=a.ClientID)
				{


					SocketIO s1=null;
					for (int i=0; i<repeat;i++)
					{
						try {

							s1= BGServer.SockPoolMapWorkload.get(i1).getConnection();

							ByteBuffer bb = ByteBuffer.allocate(8);
							bb.putInt(12);
							bb.putInt(1);
							s1.writeBytes(bb.array());

							byte[] request = s1.readBytes();
							int r=ByteBuffer.wrap(request).getInt();
							if (r==1)
								msgCount.incrementAndGet();
							else
								System.out.println("Error recieved ");
						} catch (Exception e) {
							// TODO Auto-generated catch block
							System.out.println("Pool connection Error");
						}
						BGServer.SockPoolMapWorkload.get(i1).checkIn(s1);
					}
				}
			}
		}
		////////////////////////////////////////////////////////////////////////////
		else if (testCase==22)
		{
			int count=0;
			int index;
			for (int i=0; i<numRequests;i++)
			{
				index =BGServer.getIndexForWorkToDo();
				count++;
				ByteBuffer req = ByteBuffer.allocate(4);
				req.putInt(21);

				BGServer.requestsToProcess.get(index).add(new TokenObject(req.array()));
//				BGServer.requestsSemaphores[index].release();
				BGServer.requestsSemaphores.get(index).release();
			}

		}

		///////////////////////////////////////////////////////////////////////////



	}
	public static void testCase1_1()

	{

		// create a socket pool with UnitTestServer. Check-in and out sockets for NumThreads
		testCase=11;
		new UnitTestServer().start();
		String server= "127.0.0.1:"+  UnitTestServer.port;
		try {
			serverSemaphore.acquire();
		} catch (InterruptedException e1) {
			System.out.println("Error Server Semaphore Acquire");
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		p1= new SockIOPool(server,UnitTest.NumSockets);
		UnitTest[] threads= new UnitTest[NumThreads];
		for (int i=0; i<NumThreads;i++)

		{
			threads[i]= new UnitTest();

		}
		for (int i=0; i<NumThreads;i++)

		{
			threads[i].start();

		}

		for (int i=0; i<NumThreads;i++)

		{
			try {
				threads[i].join();
			} catch (InterruptedException e) {

				System.out.println("Error Join");
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		System.out.println("Num of available sockets at the end= " + p1.availPool.size());
		//System.out.println("Num of busy sockets at the end= " + p1.busyPool.size());
		//System.out.println("Num of dead sockets at the end= " + p1.deadPool.size());
		p1.shutDown();



	}

	public static void testCase1_2()

	{ 	// Intiate a BGServer with Request Handlers. Each thread send requests for p times and wait for response from the handler

		testCase=12;
		UnitTest.NumSockets=3;
		UnitTest.NumThreads=3000;
		UnitTest.repeat=300;



		BGServer b = new BGServer(0,2,-1,-1,UnitTestServer.port, -1,UnitTest.NumSockets,12);

		try {
			UnitTest.serverSemaphore.acquire();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println("Error Semaphore acquire test case 1.2");
			e1.printStackTrace();
		}

		String server= "127.0.0.1:"+  UnitTestServer.port;
		p1= new SockIOPool(server,UnitTest.NumSockets);
		UnitTest[] threads= new UnitTest[NumThreads];
		for (int i=0; i<NumThreads;i++)

		{
			threads[i]= new UnitTest();

		}
		for (int i=0; i<NumThreads;i++)

		{
			threads[i].start();

		}

		for (int i=0; i<NumThreads;i++)

		{
			try {
				threads[i].join();
			} catch (InterruptedException e) {

				System.out.println("Error Join");
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		int sum=0;
		for (int i=0; i<NumThreads;i++)
			sum=sum+threads[i].msgCount.get();

		System.out.println("Total num of msgs: " +sum);
		System.out.println("Num of available sockets at the end= " + p1.availPool.size());
	//	System.out.println("Num of busy sockets at the end= " + p1.busyPool.size());
		//System.out.println("Num of dead sockets at the end= " + p1.deadPool.size());
		p1.sendStopHandling(p1.availPool, 98);
		p1.shutDown();



	}


	public static void testCase1_3(String[] args)

	{
		// same as test case 1.2 but with multiple BGClients
		testCase=13;
		a=null;
		try
		{
			a= new InputParsing(args);
		}
		catch(Exception ex)
		{
			System.out.println("Error: Arguments are not passed correctly " + ex.getMessage());
			System.exit(1);

		}
		testCase=13;
		UnitTest.NumSockets=a.NumSockets;
		UnitTest.NumThreads=a.NumThreads;
		UnitTest.repeat=1000;





		BGServer b = new BGServer(a.ClientID,a.NumClients,a.NumMembers,a.NumThreads,a.ClientInfoMap, a.Duration,a.NumSockets,13);



		//new UnitTestServer().start()


		UnitTest[] threads= new UnitTest[NumThreads];
		for (int i=0; i<NumThreads;i++)

		{
			threads[i]= new UnitTest();

		}
		for (int i=0; i<NumThreads;i++)

		{
			threads[i].start();

		}

		for (int i=0; i<NumThreads;i++)

		{
			try {
				threads[i].join();
			} catch (InterruptedException e) {

				System.out.println("Error Join");
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		int sum=0;
		for (int i=0; i<NumThreads;i++)
			sum=sum+threads[i].msgCount.get();

		System.out.println("Total num of msgs: " +sum);

		for (int i=0;i<a.NumClients;i++)
		{
			if (i!=a.ClientID )
			{
				SockIOPool pp= BGServer.SockPoolMapWorkload.get(i);

				pp.sendStopHandling(pp.availPool, 98);
				pp.shutDown();
			}
		}


	}
	public static void testCase2_1()
	{	
		// placing r requests in the semaphore
		testCase=21;
		numSemaphores=100;
		numWorkerThreads=500;
		numRequests=1000000;
		BGServer b = new BGServer(numSemaphores,numWorkerThreads, 0, 21);
		int count=0;
		int index;
		for (int i=0; i<numRequests;i++)
		{
			index = BGServer.getIndexForWorkToDo();
			count++;
			ByteBuffer req = ByteBuffer.allocate(4);
			req.putInt(21);

			BGServer.requestsToProcess.get(index).add(new TokenObject(req.array()));
//			BGServer.requestsSemaphores[index].release();
			BGServer.requestsSemaphores.get(index).release();
		}

		for (int ii=0; ii<numSemaphores;ii++)
		{
			while (BGServer.requestsToProcess.get(ii).size()!=0)
			{
				try {
					UnitTest.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		System.out.println("Num Work done: "+ TokenWorker.workCount.get());
		BGServer.shutdown();



		/*RequestHandler handler = new RequestHandler();

	BGServer.handlers[0]=handler;
	handler.start();*/


	}

	public static void testCase2_2()
	{	
		// k threads placing r requests in the semaphore
		testCase=22;
		numSemaphores=101;
		numWorkerThreads=300;
		numRequests=3000;
		NumThreads=300;
		BGServer b = new BGServer(101,500, 0, 21);

		UnitTest[] threads= new UnitTest[NumThreads];
		for (int i=0; i<NumThreads;i++)

		{
			threads[i]= new UnitTest();

		}
		for (int i=0; i<NumThreads;i++)

		{
			threads[i].start();

		}

		for (int i=0; i<NumThreads;i++)

		{
			try {
				threads[i].join();
			} catch (InterruptedException e) {

				System.out.println("Error Join");
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		System.out.println("Done creating requests. Waiting for workers to finish");

		for (int ii=0; ii<numSemaphores;ii++)
		{
			while (BGServer.requestsToProcess.get(ii).size()!=0)
			{
				try {
					UnitTest.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		System.out.println("Num Work done: "+ TokenWorker.workCount.get());
		BGServer.shutdown();



		/*RequestHandler handler = new RequestHandler();

	BGServer.handlers[0]=handler;
	handler.start();*/


	}
}
