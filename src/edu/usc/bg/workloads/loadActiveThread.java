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


package edu.usc.bg.workloads;
/**
 * this thread is used to show that the load is alive.
 * @author barahman
 *
 */
public class loadActiveThread extends Thread{
	int cnt =0;
	boolean exit = false;
	public loadActiveThread(){
		
	}
	
	public void setExit(){
		exit = true;
	}
	public void run(){
		while(!exit){
			try {
				Thread.sleep(10000);
				cnt++;
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
			System.out.println((cnt*10)+" Seconds:  Load is in progress");
		}
		System.out.println("state thread came out of while");
	}

}
