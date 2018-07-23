/**                                                                                                                                                                                
 * Copyright (c) 2013 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Ujwala Tambe and Pranit Mhatre                                                                                                                           
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

package azure;

public interface WindowsAzureClientConstants
{
    /**
     * The class to use as the SQL Server driver.
     */
    String DRIVER_CLASS = "db.driver";
    String DRIVER_CLASS_DEFAULT = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    /**
     * The URL to connect to the database.
     */
    String CONNECTION_URL = "db.url";
    String CONNECTION_URL_DEFAULT = "jdbc:sqlserver://td16o85xyg.database.windows.net:1433;database=UP599";
    /**
     * The user name to use to connect to the database.
     */
    String CONNECTION_USER = "db.user";
    String CONNECTION_USER_DEFAULT = "UP599user@td16o85xyg";
    /**
     * The password to use for establishing the connection.
     */
    String CONNECTION_PASSWD = "db.passwd";
    String CONNECTION_PASSWD_DEFAULT = "UP599password";
    /**
     * PreparedStatement Constants
     */
    int SUCCESS = 0;
    int CREATEFRIENDSHIP_STATEMENT = 1;
    int GETFRIENDCOUNT_STATEMENT = 2;
    int GETPENDINGFRIENDCOUNT_STATEMENT = 3;
    int GETRESOURCECOUNT_STATEMENT = 4;
    int GETPROFILE_STATEMENT = 5;
    int GETPROFILEIMG_STATEMENT = 6;
    int GETFRNDS_STATEMENT = 7;
    int GETFRNDSIMG_STATEMENT = 8;
    int GETPENDINGFRIENDS_STATEMENT = 9;
    int GETPENDINGFRIENDSIMP_STATEMENT = 10;
    int REJECTFRIEND_STATEMENT = 11;
    int INVITEFRIEND_STATEMENT = 12;
    int THAWFRIENDSHIP_STATEMENT = 13;
    int GETTOPRESOURCES_STATEMENT = 14;
    int GETRESOURCECOMMENT_STATEMENT = 15;
    int POSTCOMMENT_STATEMENT = 16;
    int DELETECOMMENT_STATEMENT = 17;
    int QUERYPENDFSHIPS_STATEMENT = 18;
    int QUERYCONFFSHIPS_STATEMENT = 19;
    int GETCREATEDRESOURCE_STATEMENT = 20;
}
