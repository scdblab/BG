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

import edu.usc.bg.base.DB;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import azure.utilities.BlobManager;

public abstract class WindowsAzureClientBase extends DB implements WindowsAzureClientConstants
{
    protected boolean connected = false;
    protected Connection connection;
    protected String query;
    protected PreparedStatement preparedStatement;
    protected ResultSet rs;
    protected Properties properties;
    protected ConcurrentMap<Integer, PreparedStatement> newCachedStatements;
    protected BlobManager blobManager = new BlobManager();

    /**
     * The method drops indexes.
     */
    public static void dropIndex(Statement statement, String indexName, String tableName)
    {
        try
        {
            statement.executeUpdate("DROP INDEX " + indexName + " ON " + tableName);
        }
        catch (SQLException ex)
        {
            Logger.getLogger(WindowsAzureClientBase.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in dropping sequence " + indexName + "!");
        }
    }

    /**
     * The method drops sequences.
     */
    public static void dropSequence(Statement statement, String seqName)
    {
        try
        {
            statement.executeUpdate("DROP SEQUENCE " + seqName);
        }
        catch (SQLException ex)
        {
            Logger.getLogger(WindowsAzureClientBase.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in dropping sequence " + seqName + "!");
        }
    }

    /**
     * The method drops tables.
     */
    public static void dropTable(Statement st, String tableName)
    {
        try
        {
            st.executeUpdate("DROP TABLE " + tableName);
        }
        catch (SQLException ex)
        {
            Logger.getLogger(WindowsAzureClientBase.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in dropping table " + tableName + "!");
        }
    }

    /**
     * The method fetches the PreparedStatements from cache. If not found, a new
     * Statement is created and added to cache.
     */
    protected void internalCreateAndCacheStatement(int stmttype, String query) throws SQLException
    {
        if ((preparedStatement = newCachedStatements.get(stmttype)) == null)
        {
            preparedStatement = connection.prepareStatement(query);
            newCachedStatements.put(stmttype, preparedStatement);
        }
    }
}
