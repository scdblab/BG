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
package applicationCache;


public interface JdbcDBClientConstants {

	/** The class to use as the jdbc driver. */
	public static final String DRIVER_CLASS = "db.driver";

	/** The URL to connect to the database. */
	public static final String CONNECTION_URL = "db.url";

	/** The user name to use to connect to the database. */
	public static final String CONNECTION_USER = "db.user";

	/** The password to use for establishing the connection. */
	public static final String CONNECTION_PASSWD = "db.passwd";

	/** The code to return when the call succeeds. */
	public static final int SUCCESS = 0;

	
	/** The name of the property for the cache server host name. */
	public static final String CACHE_SERVER_HOST="cachehostname";
	
	/** The name of the property for the cache server port. */
	public static final String CACHE_SERVER_PORT="cacheport";
	
	/** Whether the client starts and stops the cache server. */
	public static final String MANAGE_CACHE_PROPERTY = "managecache";

	/** Whether the client starts and stops the cache server. */
	public static final String MANAGE_CACHE_PROPERTY_DEFAULT = "false";
		
	/** The name of the property for the TTL value. */
	public static final String TTL_VALUE="ttlvalue";
	
	/** The name of the property for the TTL value. */
	public static final String TTL_VALUE_DEFAULT="0";
	
	/** The name of the property for the cache server host name. */
	public static final String CACHE_SERVER_HOST_DEFAULT="127.0.0.1";
	
	/** The name of the property for the cache server port. */
	public static final String CACHE_SERVER_PORT_DEFAULT="4343";
	
	/** The name of the property for enabling message compression. */
	public static final String ENABLE_COMPRESSION_PROPERTY="compress";
	
	/** The name of the property for enabling message compression. */
	public static final String ENABLE_COMPRESSION_PROPERTY_DEFAULT="false";
	
		
}