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

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;

import org.apache.log4j.PropertyConfigurator;


public class testMemcached {
	
	private static final String LOGGER = "ERROR, A1";

	public static void main(String [] args)
	{
		String value = "mytestvalue1";
		String testKey = "testKey1";
		String retrieved = null;
		
		
		try {
			System.setProperty("net.spy.log.LoggerImpl",
					  "net.spy.memcached.compat.log.SunLogger");
			Logger.getLogger("net.spy.memcached").setLevel(Level.WARNING);
					
			
			MemcachedClient client = new MemcachedClient(AddrUtil.getAddresses("10.0.0.124" + ":" + 11211));
			MemcachedClient client2 = new MemcachedClient(AddrUtil.getAddresses("10.0.0.124" + ":" + 11211));
						
			client.set(testKey, 0, value);
			retrieved = (String) client.get(testKey);
			
			System.out.println("Stored: " + value + ", Retrieved: " + retrieved);
			
			client.delete(testKey);
			retrieved = (String) client.get(testKey);
			if( retrieved != null )
			{
				System.out.println("Error. This should be null. Instead, retrieved: " + retrieved);
			}
			else
			{
				System.out.println("Delete successful");
			}
			
			client.shutdown();
			
			client2.set(testKey, 0, value);
			retrieved = (String) client2.get(testKey);
			System.out.println("Stored: " + value + ", Retrieved: " + retrieved);
			client2.shutdown();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

}
