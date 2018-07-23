package edu.usc.bg.server;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;


public class InputParsing {
	/*
	  -p numclients=3

			-p clients=IP1:Port1,IP2:Port2,IP3:Port3

			-p clientid=1

			-p nummembers=1001

			-p numrequests = 1330  (It generates 1330 requests for memberid 0 to 1329)
	 */
	public int NumClients, ClientID, NumMembers, NumRequests, NumThreads,Duration, NumSockets;
	public ConcurrentHashMap<Integer, ClientInfo> ClientInfoMap= new ConcurrentHashMap<Integer, ClientInfo>();
	InputParsing(String [] args)

	{
		parseInput(args);
		checkErrors();
	}



	public void checkErrors()
	{
		//- If numrequests > nummembers then report error and exit

		if (NumRequests>NumMembers)
		{
			System.out.println("Error: Number of request > Number of members");
			System.exit(1);
		}

		//If IP:Port are duplicates then report error and exit

		for (int i=0; i< ClientInfoMap.size();i++)
			for (int j=i+1; j<ClientInfoMap.size();j++)
			{
				if (ClientInfoMap.get(i).getIP().equals(ClientInfoMap.get(j).getIP())&& ClientInfoMap.get(i).getPort()== ClientInfoMap.get(j).getPort())
				{
					System.out.println("IP:Port Duplicates");
					System.exit(1);
				}
			}


		//The number of comma seperated IP:Port in the cleints must equal numclients.  Otherwise, report error and exit!

		if (ClientInfoMap.size()!= NumClients)
		{
			System.out.println("Error: Number of Clients not Equal Number of (IP,Port) pairs");
			System.exit(1);
		}
		//If clientid >= numclients then report error and exit

		if (ClientID>= NumClients)
		{
			System.out.println("Error: Client ID greater than Number of Clients");
			System.exit(1);	
		}
	}



	public int getIntInputValues(String s)
	{
		String value="-1";
		for (int i=0; i<s.length();i++)
		{
			if (s.charAt(i)== '=')
			{
				value= s.substring(i+1);
			}
		}
		return Integer.parseInt(value);
	}

	public void PopulateClientInfo(String s)
	{
		//clients=IP1:Port1,IP2:Port2,IP3:Port3
		try {
			int count=0;
			for (int n=0;n<s.length();n++)
			{
				if (s.charAt(n)==':')
					count++;
			}
			if (count!=NumClients)
			{
				System.out.println("Error Number of Clients not match client pairs");
				System.exit(1);
			}
			int j=s.indexOf('=');
			String s2= s.substring(j+1);

			String IP;
			int i2,i3, port;
			int i=0;
			for (i=0; i< NumClients-1;i++)
			{
				i2=s2.indexOf(':');
				i3= s2.indexOf(',');

				IP= s2.substring(0, i2);
				//System.out.println("IP= " + IP);
				port= Integer.parseInt(s2.substring(i2+1, i3));
				//System.out.println("port= " + port);
				s2=s2.substring(i3+1, s2.length());

				ClientInfo a = new ClientInfo(IP,port,i);
				ClientInfoMap.put(i, a);

			}
			i2=s2.indexOf(':');
			IP= s2.substring(0,i2);
			port= Integer.parseInt(s2.substring(i2+1,s2.length()));
			ClientInfo a = new ClientInfo(IP,port,i);

			ClientInfoMap.put(i, a);
		}
		catch(Exception ex)
		{

			System.out.println("Error: Client IP, Port are not Passed correctly");
			System.exit(1);
		}


	}
	public void parseInput(String[] args)
	{
		NumClients= getIntInputValues(args[1]);
		ClientID=getIntInputValues(args[5]);
		NumMembers=getIntInputValues(args[7]);
		NumRequests=getIntInputValues(args[9]);
		NumThreads= getIntInputValues(args[11]);
		Duration= getIntInputValues(args[13]);
		NumSockets= getIntInputValues(args[15]);

		//System.out.println();
		PopulateClientInfo(args[3]);


		/*
		 -p numclients=3

			-p clients=IP1:Port1,IP2:Port2,IP3:Port3

			-p clientid=1

			-p nummembers=1001

			-p numrequests = 1330  (It generates 1330 requests for memberid 0 to 1329)

			Error checking:

			- If numrequests > nummembers then report error and exit

			- If IP:Port are duplicates then report error and exit

			- The number of comma seperated IP:Port in the cleints must equal numclients.  Otherwise, report error and exit!
			- If clientid >= numclients then report error and exit
		 */


	}


}
