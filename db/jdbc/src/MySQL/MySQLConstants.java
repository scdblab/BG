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
package MySQL;

public interface MySQLConstants {

	/** The class to use as the jdbc driver. */
	public static final String DRIVER_CLASS = "db.driver";
	public static final String DEFAULT_DRIVER = "com.mysql.jdbc.Driver";
	
	/** The URL to connect to the database. */
	public static final String CONNECTION_URL = "db.url";
	public static final String DEFAULT_URL = "jdbc:mysql://localhost:3306/cosar";

	/** The user name to use to connect to the database. */
	public static final String CONNECTION_USER = "db.user";
	public static final String DEFAULT_USER = "root";

	/** The password to use for establishing the connection. */
	public static final String CONNECTION_PASSWD = "db.passwd";
	public static final String DEFAULT_PASSWD = "111111";
	
	/** The path to the file system for BG images. */
	public static final String FS_PATH = "db.fspath";
	
	/** Whether to call refresh on this materialized view on every relevant update */
	public static final String REFRESH_VIEWPROFILE_MV_PROP = "refresh.viewprofile";
	public static final String REFRESH_VIEWPROFILE_MV_DEFAULT = "false";
	
	/** Maximum memory that Ehcache is configured with. */
	public static final String MAX_CACHE_MEMORY_PROPERTY = "maxcachememory";
    /** Default maximum memory. */
    public static final String MAX_CACHE_MEMORY_DEFAULT = "3500";

    public static final String EH_CACHE_MODE_PARAM_DEFAULT = "true";
    public static final String EH_CACHE_MODE_PARAM = "ehcache";

    /** The code to return when the call succeeds. */
	public static final int SUCCESS = 0;
	
	
	
	
	
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




	
}	

