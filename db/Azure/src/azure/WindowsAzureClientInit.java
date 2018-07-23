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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class WindowsAzureClientInit extends WindowsAzureClientBase
{
    /**
     * The method sets up the connection with the database server.
     *
     * @return true if the connection with the server is established
     * successfully.
     */
    @Override
    public boolean init()
    {
    	File file = new File("err.txt");
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(file);
			PrintStream ps = new PrintStream(fos);
			System.setErr(ps);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        if (connected)
        {
            System.out.println("Client connection already initialized.");
            return true;
        }
        properties = getProperties();
        String driver = properties.getProperty(DRIVER_CLASS, DRIVER_CLASS_DEFAULT);
        String url = properties.getProperty(CONNECTION_URL, CONNECTION_URL_DEFAULT);
        String user = properties.getProperty(CONNECTION_USER, CONNECTION_USER_DEFAULT);
        String passwd = properties.getProperty(CONNECTION_PASSWD, CONNECTION_PASSWD_DEFAULT);
        try
        {
            if (driver != null)
            {
                Class.forName(driver);
            }
            connection = DriverManager.getConnection(url, user, passwd);
            System.out.println("Connection established successfully!");
            connected = true;
            newCachedStatements = new ConcurrentHashMap<>();
            return true;
        }
        catch (ClassNotFoundException | SQLException ex)
        {
            Logger.getLogger(WindowsAzureClientInit.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in creating a connection with the database!");
            return false;
        }
    }

    /**
     * The method closes the connection with the database server.
     */
    @Override
    public void cleanup(boolean warmup)
    {
        try
        {
            connection.close();
            System.out.println("Connection closed successfully!");
        }
        catch (SQLException ex)
        {
            Logger.getLogger(WindowsAzureClientInit.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in closing connection!");
        }
    }

    /**
     * The method creates the required schema.
     */
    @Override
    public void createSchema(Properties props)
    {
        Statement statement;
        try
        {
            statement = connection.createStatement();
                     
            //Drop Indexes
            dropIndex(statement, "CONFFRIENDSHIP_INVITEEID", "CONFFRIENDSHIP");
            dropIndex(statement, "CONFFRIENDSHIP_INVITERID", "CONFFRIENDSHIP");
            dropIndex(statement, "PENDFRIENDSHIP_INVITEEID", "PENDFRIENDSHIP");
            dropIndex(statement, "PENDFRIENDSHIP_INVITERID", "PENDFRIENDSHIP");
            dropIndex(statement, "MANIPULATION_RID", "MANIPULATION");
            dropIndex(statement, "MANIPULATION_CREATORID", "MANIPULATION");
            dropIndex(statement, "RESOURCES_WALLUSERID", "RESOURCES");
            dropIndex(statement, "RESOURCE_CREATORID", "RESOURCES");
            
             //Drop Tables
            dropTable(statement, "CONFFRIENDSHIP");
            dropTable(statement, "PENDFRIENDSHIP");
            dropTable(statement, "MANIPULATION");
            dropTable(statement, "RESOURCES");
            dropTable(statement, "USERS");
            
            //Create Tables
            statement.executeUpdate("CREATE TABLE CONFFRIENDSHIP " + "(INVITERID INTEGER NOT NULL, " + "INVITEEID INTEGER NOT NULL, " + "PRIMARY KEY (INVITERID, INVITEEID))");
            statement.executeUpdate("CREATE TABLE PENDFRIENDSHIP " + "(INVITERID INTEGER NOT NULL, " + "INVITEEID INTEGER NOT NULL, " + "PRIMARY KEY (INVITERID, INVITEEID))");
            statement.executeUpdate("CREATE TABLE MANIPULATION " + "(MID INTEGER NOT NULL, " + "MODIFIERID INTEGER NOT NULL, " + "RID INTEGER NOT NULL, " + "CREATORID INTEGER NOT NULL, " + "TIMESTAMP VARCHAR(200), " + "TYPE VARCHAR(200), " + "CONTENT VARCHAR(200), " + "PRIMARY KEY (MID,RID))");
            statement.executeUpdate("CREATE TABLE RESOURCES " + "(RID INTEGER NOT NULL , " + "CREATORID INTEGER NOT NULL, " + "WALLUSERID INTEGER NOT NULL, " + "TYPE VARCHAR(200), " + "BODY VARCHAR(200), " + "DOC VARCHAR(200), " + "PRIMARY KEY (RID))");
            statement.executeUpdate("CREATE TABLE USERS " + "(USERID INTEGER NOT NULL , " + "USERNAME VARCHAR(200), " + "PW VARCHAR(200), " + "FNAME VARCHAR(200), " + "LNAME VARCHAR(200), " + "GENDER VARCHAR(200)," + "DOB VARCHAR(200), " + "JDATE VARCHAR(200), " + "LDATE VARCHAR(200), " + "ADDRESS VARCHAR(200)," + "EMAIL VARCHAR(200), " + "TEL VARCHAR(200), " + "PRIMARY KEY (USERID))");
          
            //Add Foreign Keys
            statement.executeUpdate("ALTER TABLE CONFFRIENDSHIP " + "ADD CONSTRAINT CONFFRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)" + "REFERENCES USERS (USERID) ON DELETE CASCADE");
            statement.executeUpdate("ALTER TABLE CONFFRIENDSHIP " + "ADD CONSTRAINT CONFFRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)" + "REFERENCES USERS (USERID) ON DELETE NO ACTION");
            statement.executeUpdate("ALTER TABLE PENDFRIENDSHIP " + "ADD CONSTRAINT PENDFRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)" + "REFERENCES USERS (USERID) ON DELETE CASCADE");
            statement.executeUpdate("ALTER TABLE PENDFRIENDSHIP " + "ADD CONSTRAINT PENDFRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)" + "REFERENCES USERS (USERID) ON DELETE NO ACTION");
            statement.executeUpdate("ALTER TABLE MANIPULATION " + "ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)" + "REFERENCES RESOURCES (RID) ON DELETE CASCADE");
            statement.executeUpdate("ALTER TABLE MANIPULATION " + "ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)" + "REFERENCES USERS (USERID) ON DELETE NO ACTION");
            statement.executeUpdate("ALTER TABLE MANIPULATION " + "ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)" + "REFERENCES USERS (USERID) ON DELETE NO ACTION");
            statement.executeUpdate("ALTER TABLE RESOURCES " + "ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)" + "REFERENCES USERS (USERID) ON DELETE CASCADE");
            statement.executeUpdate("ALTER TABLE RESOURCES " + "ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)" + "REFERENCES USERS (USERID) ON DELETE NO ACTION");
            
            //Create Indexes
            buildIndexes(null);
        }
        catch (SQLException ex)
        {
            Logger.getLogger(WindowsAzureClientInit.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in creating schema!");
        }
    }

    /**
     * The method builds indexes.
     */
    @Override
    public void buildIndexes(Properties props)
    {
        Statement statement;
        try
        {
            statement = connection.createStatement();
            
            //Create Indexes
            statement.executeUpdate("CREATE INDEX CONFFRIENDSHIP_INVITEEID ON CONFFRIENDSHIP (INVITEEID)");
            statement.executeUpdate("CREATE INDEX CONFFRIENDSHIP_INVITERID ON CONFFRIENDSHIP (INVITERID)");
            statement.executeUpdate("CREATE INDEX PENDFRIENDSHIP_INVITEEID ON PENDFRIENDSHIP (INVITEEID)");
            statement.executeUpdate("CREATE INDEX PENDFRIENDSHIP_INVITERID ON PENDFRIENDSHIP (INVITERID)");
            statement.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)");
            statement.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)");
            statement.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)");
            statement.executeUpdate("CREATE INDEX RESOURCE_CREATORID ON RESOURCES (CREATORID)");
            
            System.out.println("Indexes Built");
        }
        catch (Exception ex)
        {
            Logger.getLogger(WindowsAzureClientInit.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in building indexes!");
        }
    }
}
