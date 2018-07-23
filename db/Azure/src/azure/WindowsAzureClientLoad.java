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

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.ObjectByteIterator;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import azure.utilities.*;

public abstract class WindowsAzureClientLoad extends WindowsAzureClientInit
{
    @Override
    public int CreateFriendship(int memberA, int memberB)
    {
        int retVal = SUCCESS;
        if (memberA < 0 || memberB < 0)
        {
            return -1;
        }
        query = "INSERT INTO conffriendship values(?,?)";
        try
        {
                internalCreateAndCacheStatement(CREATEFRIENDSHIP_STATEMENT, query);
                preparedStatement.setInt(1, memberA);
                preparedStatement.setInt(2, memberB);
                preparedStatement.addBatch();
                preparedStatement.setInt(1, memberB);
                preparedStatement.setInt(2, memberA);
                preparedStatement.addBatch();
                preparedStatement.executeBatch();
        }
        catch (SQLException sx)
        {
            retVal = -2;
            sx.printStackTrace(System.out);
        }
        return retVal;
    }

    @Override
    public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage)
    {
        if (entitySet == null)
        {
            return -1;
        }
        if (entityPK == null)
        {
            return -1;
        }
        int numFields = insertImage ? values.size() - 2 : values.size();
        query = "INSERT INTO " + entitySet + " (";
        switch (entitySet)
        {
            case "users":
                query += "userid, ";
                break;
            case "resources":
                query += "rid, ";
                break;
            case "manipulation":
                query += "mid, ";
                break;
        }
        Set<Map.Entry<String, ByteIterator>> entrySet = values.entrySet();
        for (Map.Entry<String, ByteIterator> entry : entrySet)
        {
            if (entry.getKey().equalsIgnoreCase("pic") || entry.getKey().equalsIgnoreCase("tpic"))
            {
                continue;
            }
            if (numFields-- == 1)
            {
                query += entry.getKey() + ")";
            }
            else
            {
                query += entry.getKey() + ", ";
            }
        }

        numFields = insertImage ? values.size() - 2 : values.size();
        query += " VALUES (?, ";
        for (int i = 0; i < numFields; i++)
        {
            if (i == numFields - 1)
            {
                query += "?)";
            }
            else
            {
                query += "?, ";
            }
        }

        try 
        {
        	final PreparedStatement ps = connection.prepareStatement(query);
            int cnt = 1;
            ps.setString(cnt++, entityPK);
            for (Map.Entry<String, ByteIterator> entry : entrySet)
            {
                //blobs need to be inserted differently
                if (entry.getKey().equalsIgnoreCase("pic") || entry.getKey().equalsIgnoreCase("tpic"))
                {
                    try
                    {
                        byte[] profileImage = ((ObjectByteIterator) entry.getValue()).toArray();
                        blobManager.putBLOB(entry.getKey().toLowerCase(), entityPK, profileImage);
                    }
                    catch (AzureClientException ex)
                    {
                        return -2;
                    }
                }
                else
                {
                    String field = entry.getValue().toString();
                    ps.setString(cnt++, field);
                }
            }
            ps.executeUpdate();
        }
        catch (SQLException e)
        {
            System.out.println("Error in processing insert to table: " + entitySet + e);
            return -2;
        }
        return SUCCESS;
    }

    @Override
    public HashMap<String, String> getInitialStats()
    {
        HashMap<String, String> statistics = new HashMap<>();
        try 
        {
        	final Statement statement = connection.createStatement();
            //get user count
            query = "SELECT count(*) from USERS";
            rs = statement.executeQuery(query);
            if (rs.next())
            {
                statistics.put("usercount", rs.getString(1));
            }
            else
            {
                statistics.put("usercount", "0");
            }

            //get user offset
            query = "SELECT min(userid) from USERS";
            rs = statement.executeQuery(query);
            if (rs.next())
            {
                int offset = rs.getInt(1);
                //get resources per user
                query = "SELECT count(*) from RESOURCES where creatorid=" + offset;
                rs = statement.executeQuery(query);
                if (rs.next())
                {
                    statistics.put("resourcesperuser", rs.getString(1));
                }
                else
                {
                    statistics.put("resourcesperuser", "0");
                }

                //get INTEGER of friends per user
                query = "select count(*) from CONFFRIENDSHIP where inviterid=" + offset;
                rs = statement.executeQuery(query);
                if (rs.next())
                {
                    statistics.put("avgfriendsperuser", rs.getString(1));
                }
                else
                {
                    statistics.put("avgfriendsperuser", "0");
                }
                query = "select count(*) from PENDFRIENDSHIP where inviteeid=" + offset;
                rs = statement.executeQuery(query);
                if (rs.next())
                {
                    statistics.put("avgpendingperuser", rs.getString(1));
                }
                else
                {
                    statistics.put("avgpendingperuser", "0");
                }
            }
            else
            {
                statistics.put("resourcesperuser", "0");
                statistics.put("avgfriendsperuser", "0");
                statistics.put("avgpendingperuser", "0");
            }
        }
        catch (SQLException ex)
        {
            Logger.getLogger(WindowsAzureClientWriteActions.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error in calculating initial statistics!");
        }
        return statistics;
    }
}
