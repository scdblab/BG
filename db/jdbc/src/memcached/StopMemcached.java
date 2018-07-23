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

package memcached;

public class StopMemcached {
	public static void main(String[] args)
	{
		String cache_hostname = "10.0.0.124";
		String getCacheStopCmd = "C:\\PSTools\\psexec \\\\"+cache_hostname+" -u shahram -p 2Shahram C:\\memcached\\memcached.exe -d stop ";
		System.out.println("Stopping Cache: "+getCacheStopCmd);
		//this.st = new StartCOSAR(this.cache_cmd + (RaysConfig.cacheServerPort + i), "cache_output" + i + ".txt"); 
		StartProcess st = new StartProcess(getCacheStopCmd, "cache_output.txt");
		st.start();

		if( st != null )
		{
			try {
				System.out.print("Waiting for memcached to finish...");
				st.join();
				System.out.println("Done!");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
