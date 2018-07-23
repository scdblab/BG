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

/**
 * Constants used by the JDBC client.
 *
 * @author sudipto
 *
 */
public interface JdbcDBMemCachedClientConstants {

	/** The class to use as the jdbc driver. */
	public static final String DRIVER_CLASS = "db.driver";

	/** The URL to connect to the database. */
	public static final String CONNECTION_URL = "db.url";

	/** The user name to use to connect to the database. */
	public static final String CONNECTION_USER = "db.user";

	/** The password to use for establishing the connection. */
	public static final String CONNECTION_PASSWD = "db.passwd";

	/** The name of the property for the number of fields in a record. */
	public static final String FIELD_COUNT_PROPERTY="fieldcount";

	/** Default number of fields in a record. */
	public static final String FIELD_COUNT_PROPERTY_DEFAULT="10";

	/** Representing a NULL value. */
	public static final String NULL_VALUE = "NULL";

	/** The code to return when the call succeeds. */
	public static final int SUCCESS = 0;

	/** The field name prefix in the table.*/
	public static String COLUMN_PREFIX = "FIELD";
	
	/** The name of the property for the memcached server host name. */
	public static final String MEMCACHED_SERVER_HOST="cachehostname";
	
	/** The name of the property for the memcached server port. */
	public static final String MEMCACHED_SERVER_PORT="cacheport";
	
	/** Whether the client starts and stops the cache server. */
	public static final String MANAGE_CACHE_PROPERTY = "managecache";

	/** Whether the client starts and stops the cache server. */
	public static final String MANAGE_CACHE_PROPERTY_DEFAULT = "false";
	
	
	/** The name of the property for the TTL value. */
	public static final String TTL_VALUE="ttlvalue";
	
	/** The name of the property for the memcached server host name. */
	public static final String MEMCACHED_SERVER_HOST_DEFAULT="127.0.0.1";
	
	/** The name of the property for the memcached server port. */
	public static final String MEMCACHED_SERVER_PORT_DEFAULT="11211";
	
	/** The name of the property for the TTL value. */
	public static final String TTL_VALUE_DEFAULT="0";
	
	/** The name of the property for enabling message compression. */
	public static final String ENABLE_COMPRESSION_PROPERTY="compress";
	
	/** The name of the property for the memcached server host name. */
	public static final String ENABLE_COMPRESSION_PROPERTY_DEFAULT="false";
	
	/** The path to the file system for BG images. */
	public static final String FS_PATH = "db.fspath"; 
	

	/** The name of the property for maximum acceptable staleness of data to be read (in seconds). */
	public static final String MAX_STALENESS_PROPERTY="maxstaleness";
	
	/** The name of the default value for maximum acceptable staleness of data to be read (in seconds). */
	public static final String MAX_STALENESS_PROPERTY_DEFAULT="0";
	
	/** The name of the property for controlling the shutdown of the cache connection pool during cleanup. */
	public static final String CLEANUP_CACHEPOOL_PROPERTY="cleanupcachepool";
	
	/** The name of the property for shutting down the connection pool during cleanup. */
	public static final String CLEANUP_CACHEPOOL_PROPERTY_DEFAULT="true";
}