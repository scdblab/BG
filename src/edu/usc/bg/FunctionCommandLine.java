/**                                                                                                                                                                                
 * Copyright (c) 2012 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */


package edu.usc.bg;

import edu.usc.bg.base.*;
import edu.usc.bg.server.BGServer;
import edu.usc.bg.server.ClientInfo;
import edu.usc.bg.server.SocketIO;
import edu.usc.bg.workloads.CoreWorkload;

import java.text.SimpleDateFormat;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.HashSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;


/**
 * A simple command line client to a database, used for testing implemented functionalities for each data store
 */
public class FunctionCommandLine
{
	public static final String DEFAULT_DB="";

	public static void usageMessage()
	{
		System.out.println("Functionality Command Line Client");
		System.out.println("Usage: java edu.usc.bg.functionCommandLine [options]");
		System.out.println("Options:");
		System.out.println("  -P filename: Specify a property file");
		System.out.println("  -p name=value: Specify a property value");
		System.out.println("  -db classname: Use a specified DB class (can also set the \"db\" property)");
		System.out.println();
	}

	public static void help()
	{

		System.out.println("Commands:");
		System.out.println("  1.insertEntity: ");
		System.out.println("  \t For users: insertEntity entitySet entityPK username=value1 pw=value2 fname=value3 lname=value4 gender=value5 dob=value6 jdate=value7 ldate=value8 address=value9 email=value10 tel=value11");
		System.out.println("  \t For resources: insertEntity entitySet entityPK creatorid=value1 walluserid=value2 type=value3 body=value4 doc=value5");
		System.out.println("  2.viewProfile requesterID profileOwnerID image(true/false)- View a profile");
		System.out.println("  3.listFriends requesterID profileOwnerID image(true/false) filed1 filed2 ...- View list of friends for a profile");
		System.out.println("  4.viewFriendReq profileOwnerID image(true/false)- View list of pending friend requests");
		System.out.println("  5.acceptFriend  inviterID inviteeID- Accept the friend request sent to invitee by invitor");
		System.out.println("  6.rejectFriend inviterID inviteeID- Reject the friend request sent to invitee by invitor");
		System.out.println("  7.inviteFriend inviterID inviteeID- Generate a friend request from inviter to invitee");
		System.out.println("  8.viewTopKResources requesterID profileOwnerID k- Get top k resources in the profile specified");
		System.out.println("  9.viewCommentOnResource requsterID resourceCreatorID resourceID- Get all comments for a resource");
		System.out.println("  10.postCommentOnResource commentCreatorID resourceCreatorID resourceID manipulationID - post a comment created by userID on resource");
		System.out.println("  11.deleteCommentOnResource resourceCreatorID resourceID manipulationID- Delete a comment for a resource");
		System.out.println("  12.thawFriendship friendid1 friendid2- unfriend the two friends");
		System.out.println("  13.getShortestDistance requesterID profileID - computed the distance between the two member ");
		System.out.println("  14.listCommonFriends requesterID profileOwnerID h image(true/false) filed1 filed2 ...- Retrieves the list of members whom are h hops away from both requesterID and profileOwnerID");
		System.out.println("  15.listFriendsOfFriends requesterID profileOwnerID image(true/false) filed1 filed2 ...- View list of friends of the friends of the profileOwnerID");
		System.out.println("  16. RetainViewProfile");
		System.out.println("  17. RetainGetPendings");
		System.out.println("  18. RetainGetFriends");
		System.out.println("  19. RetainInviteFriend");
		System.out.println("  20. RetainAcceptFriend");
		System.out.println("  21. RetainRejectFriend");
		System.out.println("  22. RetainThawFriendship");
		System.out.println("  23. delegateViewProfile ...-view profile in delegate mode");
		System.out.println("  24.delegateListFriends ...-list friends in delegate mode");
		System.out.println("  25.delegateViewFriendReq ...-view friend request in delegate mode");
		System.out.println("  26.delegateAcceptFriend ...-accept friend in delegate mode");
		System.out.println("  27.delegateRejectFriend ...-reject friend in delegate mode");
		System.out.println("  28.delegateInviteFriend ...-invite friend in delegate mode");
		System.out.println("  29.delegateThawFriend ...-thaw friend in delegate mode");
		System.out.println("  30. quit - Quit");
	}
	
	public static void main(String[] args)
	{
	
		int argindex=0;

		Properties props=new Properties();
		Properties fileprops=new Properties();

		while ( (argindex<args.length) && (args[argindex].startsWith("-")) )
		{
			if ( (args[argindex].compareTo("-help")==0) ||
					(args[argindex].compareTo("--help")==0) ||
					(args[argindex].compareTo("-?")==0) ||
					(args[argindex].compareTo("--?")==0) )
			{
				usageMessage();
				System.exit(0);
			}

			if (args[argindex].compareTo("-db")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				props.setProperty("db",args[argindex]);
				argindex++;
			}
			else if (args[argindex].compareTo("-P")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				String propfile=args[argindex];
				argindex++;

				Properties myfileprops=new Properties();
				try
				{
					myfileprops.load(new FileInputStream(propfile));
				}
				catch (IOException e)
				{
					System.out.println(e.getMessage());
					System.exit(0);
				}

				for (Enumeration e=myfileprops.propertyNames(); e.hasMoreElements(); )
				{
					String prop=(String)e.nextElement();

					fileprops.setProperty(prop,myfileprops.getProperty(prop));
				}

			}
			else if (args[argindex].compareTo("-p")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int eq=args[argindex].indexOf('=');
				if (eq<0)
				{
					usageMessage();
					System.exit(0);
				}

				String name=args[argindex].substring(0,eq);
				String value=args[argindex].substring(eq+1);
				props.put(name,value);
				//System.out.println("["+name+"]=["+value+"]");
				argindex++;
			}
			else
			{
				System.out.println("Unknown option "+args[argindex]);
				usageMessage();
				System.exit(0);
			}

			if (argindex>=args.length)
			{
				break;
			}
		} // end while

		if (argindex!=args.length)
		{
			usageMessage();
			System.exit(0);
		}

		for (Enumeration e=props.propertyNames(); e.hasMoreElements(); )
		{
			String prop=(String)e.nextElement();

			fileprops.setProperty(prop,props.getProperty(prop));
		}

		props=fileprops;

		System.out.println("Functionality Command Line client");
		System.out.println("Type \"help\" for command line help");
		System.out.println("Start with \"help\" for usage info");

		//create a DB
		String dbname=props.getProperty("db",DEFAULT_DB);

		ClassLoader classLoader = FunctionCommandLine.class.getClassLoader();

		DB db=null;

		try 
		{
			Class dbclass = classLoader.loadClass(dbname);
			db=(DB)dbclass.newInstance();
		}
		catch (Exception e) 
		{  
			e.printStackTrace(System.out);
			System.exit(0);
		}

		db.setProperties(props);
		try
		{
			db.init();
		}
		catch (DBException e)
		{
			e.printStackTrace(System.out);
			System.exit(0);
		}
		// initialize BG
		
		Client BGClient = new Client();
		
		
		Properties BGprops = new Properties();
		Properties BGfileprops = new Properties();
		//Enums & map of enum,obj
		boolean[] inputArguments = {true, false, false, false, false, false, false, false, false};
		
	
		
		int threadCount = 1;
		int target = 0;

		// parse arguments
		if (args.length == 0) {
			
			System.out.println("Arguments are not correct");
			return;
		}
		
		BGClient.readCmdArgs(args,BGprops, inputArguments, BGfileprops);
		int numClients=Integer.parseInt(BGprops.getProperty(Client.NUM_BG_PROPERTY));
		for(int i=1; i<numClients;i++)
		{
//			new BGThread(i, args).start();
			
		}
		CoreWorkload.commandLineMode=true;
		CoreWorkload.enableLogging=false; // must set logging to false, otherwise work threads need to be created to generate log records.
		BGClient.runBG(args, null);
		
		CoreWorkload coreWorkload=(CoreWorkload)Client.workload;
		
		HashMap<String,Integer> ds=new HashMap<String,Integer>();
		System.out.println("Connected.");

		
		
		
		//main loop
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		for (;;)
		{
			try{
			//get user input
			System.out.print("> ");

			String input=null;

			try
			{
				input=br.readLine();
			}
			catch (IOException e)
			{
				e.printStackTrace(System.out);
				System.exit(1);
			}

			if (input.compareTo("")==0) 
			{
				continue;
			}

			if (input.compareTo("help")==0) 
			{
				help();
				continue;
			}

			if (input.compareTo("quit")==0)
			{
				System.out.println("Shutting down  all clients");
				// first, send stop handling request to each BGClient
				SocketIO[] BGClientSockets= new SocketIO[Client.numBGClients];
				SocketIO socket;
	
				ConcurrentHashMap<Integer, ClientInfo> clientInfoMap=BGServer.ClientInfo;

				
				try {
				for (int i=0; i<Client.numBGClients;i++)
				{
				
				
					socket = new SocketIO(new Socket (clientInfoMap.get(i).getIP(), clientInfoMap.get(i).getPort()));
					BGClientSockets[i]=socket;
					BGClientSockets[i].sendValue(999);
				
				} 
				
				for (int j=0; j<Client.numBGClients;j++)
				{
					BGClientSockets[j].sendValue(9999);
					BGClientSockets[j].closeAll();
					
				}
				
				} //try
				catch (Exception e) {
					// TODO Auto-generated catch block
					System.out.println("Error: Coordinator is not able to send shutdown requests to BGClients " + e.getMessage());
					e.printStackTrace();
				} 
				
					


				break;
			}

			String[] tokens=input.split(" ");

			long st=System.currentTimeMillis();
			//handle commands
			if (tokens[0].compareTo("insertEntity")==0)
			{
				if (tokens.length < 3)
				{
					System.out.println("Error: syntax is :");
					System.out.println("  \t For users: insertEntity entitySet entityPK username=value1 pw=value2 fname=value3 lname=value4 gender=value5 dob=value6 jdate=value7 ldate=value8 address=value9 email=value10 tel=value11");
					System.out.println("  \t For resources: insertEntity entitySet entityPK creatorid=value1 walluserid=value2 type=value3 body=value4 doc=value5");
					
				}
				else 
				{
					HashMap<String,ByteIterator> values=new LinkedHashMap<String,ByteIterator>();
					if(tokens.length>3){
						for(int i=4; i<=tokens.length; i++){
							values.put((tokens[i-1].split("=")[0]).trim(), new ObjectByteIterator((tokens[i-1].split("=")[1]).trim().getBytes())); 
						}
					}
					int ret=db.insertEntity(tokens[1].trim(), tokens[2].trim(), values, false);
					System.out.println("Return code: "+ret);
				}		  
			}
			
			else if(tokens[0].compareToIgnoreCase("isRelated")==0)
			{
				Scanner in = new Scanner(System.in);
				System.out.println("Enter First user ID:");
				int userId1 = Integer.parseInt(in.nextLine());
				 in = new Scanner(System.in);
				System.out.println("Enter Second user ID:");
				int userId2 = Integer.parseInt(in.nextLine());
				System.out.println(coreWorkload.isRelated(userId1, userId2));
				
				
			}
			else if(tokens[0].compareToIgnoreCase("ViewNotRelated")==0)
			{
				Scanner in = new Scanner(System.in);
				System.out.println("Enter  user ID:");
				int userId1 = Integer.parseInt(in.nextLine());
				System.out.println(coreWorkload.viewNotRelatedUsers(userId1));
			}
			else if(tokens[0].compareToIgnoreCase("ViewRelations")==0)
			{
				Scanner in = new Scanner(System.in);
				System.out.println("Enter  user ID:");
				int userId1 = Integer.parseInt(in.nextLine());
				ArrayList<Integer> rels=coreWorkload.viewRelations(userId1);
				System.out.println("Related users:");
				for (int i: rels)
					System.out.print(i+",");
			
				
				
			}
			else if (tokens[0].compareToIgnoreCase("delegateViewProfile")==0)
			{  
				int result=coreWorkload.doActionGetProfileDelegate(db, 0, null, null,0, false, false);
				
				if (result==1)
				{
					System.out.println("Action has been performed successfully");
				}
				else
				{
					System.out.println("Action has failed");
				}
			}
			else if (tokens[0].compareToIgnoreCase("delegateListFriends") == 0) {
				int result = coreWorkload.doActionGetFriendsDelegate(db, 0, null, null, 0, false, false);
				
				if (result == 1) {
					System.out.println("Action has been performed successfully");
				} else {
					System.out.println("Actoin has failed");
				}
			}
			else if (tokens[0].compareToIgnoreCase("delegateViewFriendReq") == 0) {
				int result = coreWorkload.doActionGetPendingsDelegate(db, 0, null, null, 0, false, false);
				
				if (result == 1) {
					System.out.println("Action has been performed successfully");
				} else {
					System.out.println("Action has failed");
				}
			}
			else if (tokens[0].compareToIgnoreCase("delegateInviteFriend") == 0) {
				int result = coreWorkload.doActioninviteFriendDelegate(db, 0, null, null, 0, null, null, false, false);
				if (result == 1) {
					System.out.println("Action has been performed successfully");
				} else {
					System.out.println("Action has failed");
				}
			}
			else if (tokens[0].compareToIgnoreCase("delegateAcceptFriend") == 0) {
				int result = coreWorkload.doActionAcceptFriendsDelegate(db, 0, null, null, 0, null, null, 0, false, false);
				if (result == 1) {
					System.out.println("Action has been performed successfully");
				} else {
					System.out.println("Action has failed");
				}
			}
			else if (tokens[0].compareToIgnoreCase("delegateRejectFriend") == 0) {
				int result = coreWorkload.doActionRejectFriends(db, 0, null, null, 0, null, null, 0, false, false);
				if (result == 1) {
					System.out.println("Action has been performed successfully");
				} else {
					System.out.println("Action has failed");
				}
			}
			else if (tokens[0].compareToIgnoreCase("delegateThawFriend") == 0) {
				int result = coreWorkload.doActionUnFriendFriends(db, 0, null, null, 0, null, null, 0, false, false);
				if (result == 1) {
					System.out.println("Action has been performed successfully");
				} else {
					System.out.println("Action has failed");
				}
			}


else if (tokens[0].compareTo("RetainViewProfile")==0)
			{  
				boolean insertImage=true;
				System.out.print("Insert Image (y or n)? ");
				Scanner in = new Scanner(System.in);
				
				String res = in.nextLine();
				if (res.equals("y"))
				{
					insertImage=true;
					
				}
				else
				{
					insertImage=false;
				}
				int result=coreWorkload.doActionGetProfileRetain(db, 0, null, null,0, insertImage, true);
				
				if (result==1)
				{
					System.out.println("Action has been performed successfully");
				}
				else
				{
					System.out.println("Action has failed");

					
				}

					
			}
			else if (tokens[0].compareTo("RetainGetPendings")==0)
			{  
			
				
				int result=coreWorkload.doActionGetPendingsRetain(db, 0, null, null,0,false, true);
				
				if (result==1)
				{
					System.out.println("Action has been performed successfully");
				}
				else
				{
					System.out.println("Action has failed");

					
				}
			}
			
			else if (tokens[0].compareTo("RetainGetFriends")==0)
			{  
			
				
				int result=coreWorkload.doActionGetFriendsRetain(db, 0, null, null,0,false, true);
				
				if (result==1)
				{
					System.out.println("Action has been performed successfully");
				}
				else
				{
					System.out.println("Action has failed");

					
				}
			}
			else if (tokens[0].compareTo("RetainThawFriendship")==0)
			{  
			
				
				int result;
				
					result = coreWorkload.doActionUnFriendFriendsRetain(db, 0, null, null,0,ds,ds,0,false, false);
				
				
				if (result==1)
				{
					System.out.println("Action has been performed successfully");
				}
				else
				{
					System.out.println("Action has failed");

					
				}
			}
			else if (tokens[0].compareTo("RetainInviteFriend")==0)
			{  
			
				
				int result;
				
					result = coreWorkload.doActioninviteFriendRetain(db, 0, null, null,0,ds,ds,false, false);
				
				
				if (result==1)
				{
					System.out.println("Action has been performed successfully");
				}
				else
				{
					System.out.println("Action has failed");

					
				}
			}
			else if (tokens[0].compareTo("RetainAcceptFriend")==0)
			{  
			
				
				int result=coreWorkload.doActionAcceptFriendsRetain(db, 0, null, null,0,ds,ds,0,false, false);
				
				if (result==1)
				{
					System.out.println("Action has been performed successfully");
				}
				else
				{
					System.out.println("Action has failed");

					
				}
			}
			else if (tokens[0].compareTo("RetainRejectFriend")==0)
			{  
			
				
				int result=coreWorkload.doActionRejectFriendsRetain(db, 0, null, null,0,ds,ds,0,false, false);
				
				if (result==1)
				{
					System.out.println("Action has been performed successfully");
				}
				else
				{
					System.out.println("Action has failed");

					
				}
			}
			else if (tokens[0].compareTo("viewProfile")==0)
			{  
				
			
				if (tokens.length != 4)
				{
					System.out.println("Error: syntax is \"viewProfile requesterID profileOwnerID image(true/false)\"");
				}
				else 
				{
					HashMap<String,ByteIterator> result=new HashMap<String,ByteIterator>();
					//assuming no images have been inserted for the users
					//the last true is because we are in testing mode and we want the image if any to be written to the file
					int ret=db.viewProfile(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]), result, Boolean.parseBoolean(tokens[3]), true);
					System.out.println("Return code: "+ret);
					for (Map.Entry<String,ByteIterator> ent : result.entrySet())
					{
						System.out.println(ent.getKey()+"="+ent.getValue()+" ");
						if(ent.getKey().equalsIgnoreCase(new String("pic"))){
							String strFilePath = "demo.bmp"; 
						     try{
						      FileOutputStream fos = new FileOutputStream(strFilePath);
						      fos.write(((ObjectByteIterator)(ent.getValue())).toArray());
						      fos.close();
						     }catch(FileNotFoundException ex){
						      System.out.println("FileNotFoundException : " + ex);
						     }catch(IOException ioe){
						      System.out.println("IOException : " + ioe);
						     }
						}
					}
				}		  
			}
			else if (tokens[0].compareTo("listFriends")==0)
			{
				if (tokens.length<4)
				{
					System.out.println("Error: syntax is \"listFriends requesterID profileOwnerID image(true/false) field1 field2 ...\"");
				}
				else 
				{
					Set<String> fields=null;

					if (tokens.length>4)
					{
						fields=new HashSet<String>();

						for (int i=4; i<tokens.length; i++)
						{
							fields.add(tokens[i]);
						}
					}

					Vector<HashMap<String,ByteIterator>> results=new Vector<HashMap<String,ByteIterator>>();
					//always get friends from mongo as a list not one by one in a for loop
					//assuming no images have been inserted for the users
					int ret=db.listFriends(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]),fields,results, Boolean.parseBoolean(tokens[3]), true );
					System.out.println("Return code: "+ret);
					int record=0;
					if (results.size()==0)
					{
						System.out.println("0 records");
					}
					else
					{
						System.out.println("--------------------------------");
					}
					for (HashMap<String,ByteIterator> result : results)
					{
						System.out.println("Friend "+(record++));
						for (Map.Entry<String,ByteIterator> ent : result.entrySet())
						{
							System.out.print(ent.getKey()+"="+ent.getValue()+" ");
							if(ent.getKey().equalsIgnoreCase(new String("tpic"))){
							String strFilePath = "tdemo.bmp";
							   
						     try{
						      FileOutputStream fos = new FileOutputStream(strFilePath);
						      fos.write(ent.getValue().toArray());
						      fos.close();
						     }catch(FileNotFoundException ex){
						      System.out.println("FileNotFoundException : " + ex);
						     }catch(IOException ioe){
						      System.out.println("IOException : " + ioe);
						     }
						}
						}
						System.out.println("\n--------------------------------");
					}
				}		  
			}
			else if (tokens[0].compareTo("viewFriendReq")==0)
			{
				if (tokens.length<3)
				{
					System.out.println("Error: syntax is \"viewFriendReq profileOwnerID image(true/false)\"");
				}
				else 
				{
					Vector<HashMap<String,ByteIterator>> results=new Vector<HashMap<String,ByteIterator>>();
					//always get list of pending requests as a list not one by one in a for loop
					//assuming no images have been inserted for the users
					int ret=db.viewFriendReq(Integer.parseInt(tokens[1]), results,  Boolean.parseBoolean(tokens[2]), true);
					System.out.println("Return code: "+ret);
					int record=0;
					if (results.size()==0)
					{
						System.out.println("0 records");
					}
					else
					{
						System.out.println("--------------------------------");
					}
					for (HashMap<String,ByteIterator> result : results)
					{
						System.out.println("Pending Friend "+(record++));
						for (Map.Entry<String,ByteIterator> ent : result.entrySet())
						{
							System.out.print(ent.getKey()+"="+ent.getValue()+" ");
						}
						System.out.println("\n--------------------------------");
					}
				}		  
			}
			else if (tokens[0].compareTo("acceptFriend")==0)
			{
				if (tokens.length != 3)
				{
					System.out.println("Error: syntax is \"acceptFriend  inviterID inviteeID\"");
				}
				else 
				{
					int ret=db.acceptFriend(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]));
					System.out.println("Return code: "+ret);

				}		  
			}
			else if (tokens[0].compareTo("rejectFriend")==0)
			{
				if (tokens.length!=3)
				{
					System.out.println("Error: syntax is \"rejectFriend inviterID inviteeID\"");
				}
				else 
				{
					int ret=db.rejectFriend(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]));
					System.out.println("Return code: "+ret);
				}		  
			}
			else if (tokens[0].compareTo("inviteFriend")==0)
			{
				if (tokens.length!=3)
				{
					System.out.println("Error: syntax is \"inviteFriend inviterID inviteeID\"");
				}
				else 
				{
					int ret=db.inviteFriend(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]));
					System.out.println("Return code: "+ret);
				}		  
			}
			else if (tokens[0].compareTo("thawFriendship")==0)
			{
				if (tokens.length!=3)
				{
					System.out.println("Error: syntax is \"thawFriendship friendid1 friendid2\"");
				}
				else 
				{
					int ret=db.thawFriendship(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]));
					System.out.println("Return code: "+ret);
				}		  
			}
			else if (tokens[0].compareTo("viewTopKResources")==0)
			{
				if (tokens.length!=4)
				{
					System.out.println("Error: syntax is \"viewTopKResources requesterID profileOwnerID k\"");
				}
				else 
				{
					Vector<HashMap<String,ByteIterator>> results=new Vector<HashMap<String,ByteIterator>>();
					int ret=db.viewTopKResources(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]), results);
					System.out.println("Return code: "+ret);
					int record=0;
					if (results.size()==0)
					{
						System.out.println("0 records");
					}
					else
					{
						System.out.println("--------------------------------");
					}
					for (HashMap<String,ByteIterator> result : results)
					{
						System.out.println("Resource "+(record++));
						for (Map.Entry<String,ByteIterator> ent : result.entrySet())
						{
							System.out.print(ent.getKey()+"="+ent.getValue()+" ");
						}
						System.out.println("\n--------------------------------");
					}
				}		  
			}
			else if (tokens[0].compareTo("viewCommentOnResource")==0)
			{
				if (tokens.length!=4)
				{
					System.out.println("Error: syntax is \"viewCommentOnResource requsterID resourceCreatorID resourceID\"");
				}
				else 
				{
					Vector<HashMap<String,ByteIterator>> results=new Vector<HashMap<String,ByteIterator>>();
					int ret=db.viewCommentOnResource(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]),results ); 
					System.out.println("Return code: "+ret);
					int record=0;
					if (results.size()==0)
					{
						System.out.println("0 records");
					}
					else
					{
						System.out.println("--------------------------------");
					}
					for (HashMap<String,ByteIterator> result : results)
					{
						System.out.println("Comment "+(record++));
						for (Map.Entry<String,ByteIterator> ent : result.entrySet())
						{
							System.out.print(ent.getKey()+"="+ent.getValue()+" ");
						}
						System.out.println("\n--------------------------------");
					}
				}		  
			}
			else if (tokens[0].compareTo("postCommentOnResource")==0)
			{
				if (tokens.length!=5)
				{
					System.out.println("Error: syntax is \"postCommentOnResource commentCreatorID resourceCreatorID resourceID manipulationID \"");
				}
				else 
				{
					HashMap<String,ByteIterator> commentValues = new HashMap<String, ByteIterator>();
					
					//insert random timestamp, type and content for the comment created
					String[] fieldName = {"timestamp", "type", "content"};
					for (int i = 1; i <= 3; ++i)
					{
						String fieldKey = fieldName[i-1];
						ByteIterator data;
						if(1 == i){
							Date date = new Date();
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
							String dateString = sdf.format(date);
							data = new ObjectByteIterator(dateString.getBytes()); // Timestamp.
						}else{
							data = new RandomByteIterator(100); // Other fields.
						}
						commentValues.put(fieldKey, data);
					}
					commentValues.put("mid", new ObjectByteIterator(tokens[4].getBytes()));
					
					
					int ret=db.postCommentOnResource(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]), commentValues);
					System.out.println("Return code: "+ret);
				}		  
			}

			else if (tokens[0].compareTo("deleteCommentOnResource")==0)
			{
				if (tokens.length!=4)
				{
					System.out.println("Error: syntax is \"deleteCommentOnResource resourceCreatorID resourceID manipulationID \"");
				}
				else 
				{
					int ret=db.delCommentOnResource(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), Integer.parseInt(tokens[3]));
					System.out.println("Return code: "+ret);
				}		  
			}
//			else if (tokens[0].compareTo("getShortestDistance")==0)
//			{
//				if (tokens.length!=3)
//				{
//					System.out.println("Error: syntax is \"getShortestDistance requesterID profileID \"");
//				}
//				else 
//				{
//					int ret=db.getShortestPathLength(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]));
//					System.out.println("Return code: "+ret);
//				}
//			}
//			else if (tokens[0].compareTo("listCommonFriends")==0)
//			{
//				if (tokens.length < 5)
//				{
//					System.out.println("Error: syntax is \"listCommonFriends requesterID profileOwnerID h image(true/false) filed1 filed2 \"");
//				}
//				else 
//				{
//					Set<String> fields=null;
//
//					if (tokens.length>5)
//					{
//						fields=new HashSet<String>();
//
//						for (int i=5; i<tokens.length; i++)
//						{
//							fields.add(tokens[i]);
//						}
//					}
//
//					Vector<HashMap<String,ByteIterator>> results=new Vector<HashMap<String,ByteIterator>>();
//					//always get friends from mongo as a list not one by one in a for loop
//					//assuming no images have been inserted for the users
//					int ret=db.listCommonFriends(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]),Integer.parseInt(tokens[3]),fields,results, Boolean.parseBoolean(tokens[3]), true );
//					System.out.println("Return code: "+ret);
//					int record=0;
//					if (results.size()==0)
//					{
//						System.out.println("0 records");
//					}
//					else
//					{
//						System.out.println("--------------------------------");
//					}
//					for (HashMap<String,ByteIterator> result : results)
//					{
//						System.out.println("Friend "+(record++));
//						for (Map.Entry<String,ByteIterator> ent : result.entrySet())
//						{
//							System.out.print(ent.getKey()+"="+ent.getValue()+" ");
//							if(ent.getKey().equalsIgnoreCase(new String("tpic"))){
//							String strFilePath = "tdemo.bmp";
//							   
//						     try{
//						      FileOutputStream fos = new FileOutputStream(strFilePath);
//						      fos.write(ent.getValue().toArray());
//						      fos.close();
//						     }catch(FileNotFoundException ex){
//						      System.out.println("FileNotFoundException : " + ex);
//						     }catch(IOException ioe){
//						      System.out.println("IOException : " + ioe);
//						     }
//						}
//						}
//						System.out.println("\n--------------------------------");
//					}
//				}
//				
//			}
//			else if (tokens[0].compareTo("listFriendsOfFriends")==0) 
//			{
//				if (tokens.length < 4)
//				{
//					System.out.println("Error: syntax is \"listFriendsOfFriends requesterID profileOwnerID image(true/false) filed1 filed2 \"");
//				}
//				else 
//				{
//					Set<String> fields=null;
//
//					if (tokens.length>4)
//					{
//						fields=new HashSet<String>();
//
//						for (int i=4; i<tokens.length; i++)
//						{
//							fields.add(tokens[i]);
//						}
//					}
//
//					Vector<HashMap<String,ByteIterator>> results=new Vector<HashMap<String,ByteIterator>>();
//					//always get friends from mongo as a list not one by one in a for loop
//					//assuming no images have been inserted for the users
//					int ret=db.listFriendsOfFriends(Integer.parseInt(tokens[1]),Integer.parseInt(tokens[2]),fields,results, Boolean.parseBoolean(tokens[3]), true );
//					System.out.println("Return code: "+ret);
//					int record=0;
//					if (results.size()==0)
//					{
//						System.out.println("0 records");
//					}
//					else
//					{
//						System.out.println("--------------------------------");
//					}
//					for (HashMap<String,ByteIterator> result : results)
//					{
//						System.out.println("Friend "+(record++));
//						for (Map.Entry<String,ByteIterator> ent : result.entrySet())
//						{
//							System.out.print(ent.getKey()+"="+ent.getValue()+" ");
//							if(ent.getKey().equalsIgnoreCase(new String("tpic"))){
//							String strFilePath = "tdemo.bmp";
//							   
//						     try{
//						      FileOutputStream fos = new FileOutputStream(strFilePath);
//						      fos.write(ent.getValue().toArray());
//						      fos.close();
//						     }catch(FileNotFoundException ex){
//						      System.out.println("FileNotFoundException : " + ex);
//						     }catch(IOException ioe){
//						      System.out.println("IOException : " + ioe);
//						     }
//						}
//						}
//						System.out.println("\n--------------------------------");
//					}
//				}
//			}
			else
			{
				System.out.println("Error: unknown command \""+tokens[0]+"\"");
			}

			System.out.println((System.currentTimeMillis()-st)+" ms");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		}
		
	}

}
 class BGThread extends Thread
{
	 int threadID;
	 String arguments[];
	 BGThread(int id, String args[])
	 {
		 threadID=id;
		 arguments= new String[args.length];
		 for (int i=0;i<args.length;i++)
		 {
			 if (args[i].contains(Client.MACHINE_ID_PROPERTY))
			 {
				 arguments[i]=Client.MACHINE_ID_PROPERTY+"="+threadID;
				 
			 }
			 else
				 arguments[i]=args[i];
			 
		 }
		 
	 }
	 public void run()
	 {
		 Process p1=null;
		 String run="java -Xmx1G -cp C:/Users/yaz/Documents/BGTest_Communication_Infrastructure/BG/bin;C:/Users/yaz/Documents/BGTest/BG/db/jdbc/lib/*;C:/Users/yaz/Documents/BGTest/BG/db/mongodb/lib/* edu.usc.bg.BGMainClass ";
		
				try {
					p1 = Runtime.getRuntime().exec((run+arguments));
					p1.waitFor();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		 
	 }
	
}