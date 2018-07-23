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
package relational;

/**
 * Constants used by the JDBC client.
 *
 * @author sudipto
 *
 */
public interface JdbcDBClientConstants {

	/** The class to use as the jdbc driver. */
	public static final String DRIVER_CLASS = "db.driver";

	/** The URL to connect to the database. */
	public static final String CONNECTION_URL = "db.url";

	/** The user name to use to connect to the database. */
	public static final String CONNECTION_USER = "db.user";

	/** The password to use for establishing the connection. */
	public static final String CONNECTION_PASSWD = "db.passwd";
	
	/** The path to the file system for BG images. */
	public static final String FS_PATH = "db.fspath"; 

	/** The code to return when the call succeeds. */
	public static final int SUCCESS = 0;

	/** Whether to call refresh on this materialized view on every relevant update */
	public static final String REFRESH_VIEWFRIENDREQ_MV_PROP = "refresh.friendreq";
	public static final String REFRESH_VIEWFRIENDREQ_MV_DEFAULT = "false";
	
	/** Whether to call refresh on this materialized view on every relevant update */
	public static final String REFRESH_LISTFRIENDS_MV_PROP = "refresh.listfriends";
	public static final String REFRESH_LISTFRIENDS_MV_DEFAULT = "false";
	
	/** Whether to call refresh on this materialized view on every relevant update */
	public static final String REFRESH_VIEWPROFILE_MV_PROP = "refresh.viewprofile";
	public static final String REFRESH_VIEWPROFILE_MV_DEFAULT = "false";
}