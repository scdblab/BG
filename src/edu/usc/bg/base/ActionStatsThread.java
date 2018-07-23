package edu.usc.bg.base;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.usc.bg.validator.logObject;

public class ActionStatsThread extends Thread {
	long numLocalActs;
	long numPartialActs;
	long numPartialOrLocalActs;
	int threadid;
	int machineid;
	String logDir;
	public ActionStatsThread( int id, String log, int mid  ) {
		// TODO Auto-generated constructor stub
		threadid=id;
		logDir=log;
		machineid=mid;
		numLocalActs=0;
		numPartialActs=0;
		numPartialOrLocalActs=0;
		
	}
	public void run(){
		String logtype="read";
		long []values=parseLogFiles(logtype);
		logtype="update";
		long []values2=parseLogFiles(logtype);
		numLocalActs=(values[0]+values2[0]);
		numPartialActs=(values[1]+values2[1]);
		numPartialOrLocalActs=(values[2]+values2[2]);
		
		
	}
	long[] parseLogFiles(String logtype)
	{
		long numLocalActions=0;
		long numPartialActions=0;
		long numlocalOrPartialActions=0;
		FileInputStream fstream=null;
		logObject record1=null,record2=null;
		

			try {
				fstream = new FileInputStream(logDir+"//"+logtype+machineid+"-"+threadid + ".txt");
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String prevSeq="-1";
			// Read File Line By Line
			String[] tokens,tokens2=null;
			try {
				for (String next, line = br.readLine(); line != null; line = next) 
				{
					tokens = line.split(",");
					next = br.readLine();
					if (logtype.equals("read"))
					record1 = new logObject(tokens[0], tokens[1], tokens[2],tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], "", tokens[8]);
					else
					record1 = new logObject(tokens[0], tokens[1], tokens[2],tokens[3], tokens[4], tokens[5], tokens[6], tokens[7], tokens[8], tokens[9]);

					if (next!=null)
					{
						tokens2 = next.split(",");
						if (logtype.equals("read"))
						record2 = new logObject(tokens2[0], tokens2[1], tokens2[2],tokens2[3], tokens2[4], tokens2[5], tokens2[6], tokens2[7], "", tokens2[8]);
						else
							record2 = new logObject(tokens2[0], tokens2[1], tokens2[2],tokens2[3], tokens2[4], tokens2[5], tokens2[6], tokens2[7], tokens2[8], tokens2[9]);

							
					}
					else
					{
				
						record2 = new logObject("-1", "-1","-1", "-1","-1", "-1","-1", "-1" , "", "-1");


					}
					if (record1.getActionType().equals("GetProfile"))
					{
						if (!prevSeq.equals(record1.getSeqId()))
						{
							numLocalActions++;
						}
						prevSeq=record1.getSeqId();

					}
					else if (record1.getActionType().equals("InviteFriends") ||record1.getActionType().equals("RejectFriend") )
					{
						numlocalOrPartialActions++;
						prevSeq=record1.getSeqId();

					}
					else if (record1.getActionType().equals("Unfriendfriend") ||record1.getActionType().equals("AcceptFriend") )
					{
						if (record1.getActionType().equals("AcceptFriend") && record2.getSeqId().equals(record1.getSeqId()) && record2.getRid().equals(record1.getRid()))
						{// ignoring the first record for accept friend for the invitee because this action generates 3 log records
							;
						}
						else
						{
						 if (!record1.getSeqId().equals(prevSeq)&& !record2.getSeqId().equals(record1.getSeqId()))
						{
							numPartialActions++;
						}

						else if (record2.getSeqId().equals(record1.getSeqId()))
						{
							numLocalActions++;


						}

					}
						prevSeq=record1.getSeqId();
					}
					else if (record1.getActionType().contains("GetPendingFriends") ||record1.getActionType().contains( "GetFriends") ) { // for listFriends and getPndings
						numLocalActions++;
						prevSeq=record1.getSeqId();
					}
					
					


				}
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		long []values={numLocalActions,numPartialActions,numlocalOrPartialActions};
		return values;
	}


}
