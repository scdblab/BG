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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Ujwala Tambe, Pranit Mhatre for CSCI599
 */
public abstract class WindowsAzureClientWriteActions extends WindowsAzureClientReadActions
{
    @Override
    public int acceptFriend(int inviterID, int inviteeID)
    {
        int retVal = SUCCESS;
        if (inviterID < 0 || inviteeID < 0)
        {
            return -1;
        }
        try
        {
            connection.setAutoCommit(false);
            CreateFriendship(inviterID, inviteeID);
            rejectFriend(inviterID, inviteeID);
            connection.commit();
            connection.setAutoCommit(true);
        }
        catch (SQLException ex)
        {
            retVal = -2;
            Logger.getLogger(WindowsAzureClientWriteActions.class.getName()).log(Level.SEVERE, null, ex);
        }

        return retVal;
    }

    @Override
    public int rejectFriend(int inviterID, int inviteeID)
    {
        int retVal = SUCCESS;
        if (inviterID < 0 || inviteeID < 0)
        {
            return -1;
        }

        query = "DELETE FROM pendfriendship WHERE inviterid=? and inviteeid= ?";
        try
        {
            internalCreateAndCacheStatement(REJECTFRIEND_STATEMENT, query);
            preparedStatement.setInt(1, inviterID);
            preparedStatement.setInt(2, inviteeID);
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            retVal = -2;
            sx.printStackTrace(System.out);
        }
        return retVal;
    }

    @Override
    public int inviteFriend(int inviterID, int inviteeID)
    {
        int retVal = SUCCESS;
        if (inviterID < 0 || inviteeID < 0)
        {
            return -1;
        }
        query = "INSERT INTO pendfriendship values(?,?)";
        try
        {
            internalCreateAndCacheStatement(INVITEFRIEND_STATEMENT, query);
            preparedStatement.setInt(1, inviterID);
            preparedStatement.setInt(2, inviteeID);
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            retVal = -2;
            sx.printStackTrace(System.out);
        }
        return retVal;
    }

    @Override
    public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID, HashMap<String, ByteIterator> values)
    {

        int retVal = SUCCESS;

        if (resourceCreatorID < 0 || commentCreatorID < 0 || resourceID < 0)
        {
            return -1;
        }

        query = "INSERT INTO manipulation(mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?,?,?, ?,?, ?, ?)";
        try
        {
            internalCreateAndCacheStatement(POSTCOMMENT_STATEMENT, query);
            preparedStatement.setInt(1, Integer.parseInt(values.get("mid").toString()));
            preparedStatement.setInt(2, resourceCreatorID);
            preparedStatement.setInt(3, resourceID);
            preparedStatement.setInt(4, commentCreatorID);
            preparedStatement.setString(5, values.get("timestamp").toString());
            preparedStatement.setString(6, values.get("type").toString());
            preparedStatement.setString(7, values.get("content").toString());
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            retVal = -2;
            System.out.println("Error while posting comment!");
        }

        return retVal;
    }

    @Override
    public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID)
    {
        int retVal = SUCCESS;
        if (resourceCreatorID < 0 || resourceID < 0 || manipulationID < 0)
        {
            return -1;
        }

        query = "DELETE FROM manipulation WHERE mid=? AND rid=?";
        try
        {
            internalCreateAndCacheStatement(DELETECOMMENT_STATEMENT, query);
            preparedStatement.setInt(1, manipulationID);
            preparedStatement.setInt(2, resourceID);
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            retVal = -2;
            System.out.println("Error while deleting comment!");
        }
        return retVal;
    }

    @Override
    public int thawFriendship(int friendid1, int friendid2)
    {

        int retVal = SUCCESS;
        if (friendid1 < 0 || friendid2 < 0)
        {
            return -1;
        }

        query = "DELETE FROM conffriendship WHERE (inviterid=? and inviteeid= ?) OR (inviterid=? and inviteeid= ?)";
        try
        {
            internalCreateAndCacheStatement(THAWFRIENDSHIP_STATEMENT, query);
            preparedStatement.setInt(1, friendid1);
            preparedStatement.setInt(2, friendid2);
            preparedStatement.setInt(3, friendid2);
            preparedStatement.setInt(4, friendid1);
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            retVal = -2;
            System.out.println("Error while thawing friendship!");
        }
        return retVal;
    }
}
