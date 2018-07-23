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

import java.util.Scanner;
import java.util.Vector;

import edu.usc.bg.base.Workload;


/**
 * The thread is used in rating mode and is responsible for killing the BG client 
 * It kills the BG client upon receiving a message from the coordinator reflecting that the BG client 
 * is doing poor.
 * @author barahman
 *
 */
public class KillThread extends Thread{
	Scanner _scanIn = null;
	Vector<Thread> _threads;
	Workload _workload;

	public KillThread(Scanner in, Vector<Thread> threads, 
			Workload workload){
		System.out.println("KILL THREAD CREATED");
		_scanIn = in;
		_threads = threads;
		_workload = workload;	  
	}

	public void run(){
		System.out.println("KILL THREAD STARTED");
		while(!_workload.isStopRequested()){
			if (_scanIn != null && _scanIn.hasNext() )
			{
			//read anything that comes from the shell after the simulation has started
			String msg = _scanIn.next();
			if(msg.equals("KILL")){
				System.out.println("The Shell has sent a kill message.");
				_workload.requestStop();
				System.out.println("Stop requested for workload due to KILL msg. Now eXITING!");
				System.out.println("KILLDONE");
				System.exit(0);
			}
			System.out.println("lll");
			}

		}	
	}


}