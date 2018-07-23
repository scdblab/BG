/**
 * Copyright (c) 2013 USC Database Laboratory All rights reserved.
 *
 * Authors: Ujwala Tambe and Pranit Mhatre
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */
package azure;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.ObjectByteIterator;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import azure.utilities.*;

public abstract class WindowsAzureClientReadActions extends WindowsAzureClientLoad
{
    @Override
    public int getCreatedResources(int resourceCreatorID, Vector<HashMap<String, ByteIterator>> result)
    {
        int returnValue = SUCCESS;
        if (resourceCreatorID < 0)
        {
            return -1;
        }
        query = "SELECT * FROM RESOURCES WHERE creatorid = " + resourceCreatorID;
        try
        {
            internalCreateAndCacheStatement(GETCREATEDRESOURCE_STATEMENT, query);
            rs = preparedStatement.executeQuery();
            while (rs.next())
            {
                HashMap<String, ByteIterator> values = new HashMap<>();
                //get the columns and their names
                ResultSetMetaData metaData = rs.getMetaData();
                int col = metaData.getColumnCount();
                for (int i = 1; i <= col; i++)
                {
                    String col_name = metaData.getColumnName(i);
                    String value = rs.getString(col_name);
                    if (col_name.equalsIgnoreCase("rid"))
                    {
                        col_name = "rid";
                    }
                    values.put(col_name, new ObjectByteIterator(value.getBytes()));
                }
                result.add(values);
            }
            preparedStatement.close();
        }
        catch (SQLException ex)
        {
            returnValue = -2;
            Logger.getLogger(WindowsAzureClientWriteActions.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Error while retrieving resource list!");
        }
        return returnValue;
    }

    @Override
    public int listFriends(int requesterID, int profileOwnerID, Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode)
    {
        int retVal = SUCCESS;
        if (requesterID < 0 || profileOwnerID < 0)
        {
            return -1;
        }
        try
        {
            query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, conffriendship WHERE (inviterid=? and userid=inviteeid)";
            internalCreateAndCacheStatement(GETFRNDS_STATEMENT, query);
            preparedStatement.setInt(1, profileOwnerID);
            rs = preparedStatement.executeQuery();
            while (rs.next())
            {
                HashMap<String, ByteIterator> values = new HashMap<>();
                String userid = "";
                if (fields != null)
                {
                    for (String field : fields)
                    {
                        String value = rs.getString(field);
                        if (field.equalsIgnoreCase("userid"))
                        {
                            field = "userid";
                            userid = value;
                        }
                        values.put(field, new ObjectByteIterator(value.getBytes()));
                    }
                }
                else
                {
                    //get the columns and their names
                    ResultSetMetaData rsMetaData = rs.getMetaData();
                    int col = rsMetaData.getColumnCount();
                    for (int i = 1; i <= col; i++)
                    {
                        String col_name = rsMetaData.getColumnName(i);
                        String value;
                        value = rs.getString(col_name);
                        if (col_name.equalsIgnoreCase("userid"))
                        {
                            col_name = "userid";
                            userid = value;
                        }
                        values.put(col_name, new ObjectByteIterator(value.getBytes()));
                    }
                }
                if (insertImage)
                {
                    //Get as a BLOB data-type
                    byte[] allBytesInBlob = blobManager.getBLOB(BlobManager.TPIC, userid);
                    values.put(BlobManager.TPIC, new ObjectByteIterator(allBytesInBlob));
                }
                result.add(values);
            }
            preparedStatement.close();
        }
        catch (AzureClientException | SQLException ex)
        {
            retVal = -2;
            ex.printStackTrace(System.out);
        }
        return retVal;
    }

    @Override
    public int queryConfirmedFriendshipIds(int profileId, Vector<Integer> confirmedIds)
    {
        int retVal = SUCCESS;
        query = "SELECT inviteeid from conffriendship where inviterid=" + profileId;
        if (profileId < 0)
        {
            retVal = -1;
        }
        try
        {
            internalCreateAndCacheStatement(QUERYCONFFSHIPS_STATEMENT, query);
            rs = preparedStatement.executeQuery();
            while (rs.next())
            {
                confirmedIds.add(rs.getInt(1));
            }
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            sx.printStackTrace(System.out);
            retVal = -2;
        }
        return retVal;
    }

    @Override
    public int queryPendingFriendshipIds(int inviteeid, Vector<Integer> pendingIds)
    {
        int retVal = SUCCESS;
        if (inviteeid < 0)
        {
            retVal = -1;
        }
        query = "SELECT inviterid from pendfriendship where inviteeid=" + inviteeid;
        try
        {
            internalCreateAndCacheStatement(QUERYPENDFSHIPS_STATEMENT, query);
            rs = preparedStatement.executeQuery();
            while (rs.next())
            {
                pendingIds.add(rs.getInt(1));
            }
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            sx.printStackTrace(System.out);
            retVal = -2;
        }
        return retVal;
    }

    @Override
    public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID, Vector<HashMap<String, ByteIterator>> result)
    {
        int retVal = SUCCESS;
        HashMap<String, ByteIterator> values;
        if (profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
        {
            return -1;
        }
        query = "SELECT * FROM manipulation WHERE rid = ?";
        try
        {
            internalCreateAndCacheStatement(GETRESOURCECOMMENT_STATEMENT, query);
            preparedStatement.setInt(1, resourceID);
            rs = preparedStatement.executeQuery();
            while (rs.next())
            {
                values = new HashMap<>();
                //get the number of columns and their names
                ResultSetMetaData md = rs.getMetaData();
                int col = md.getColumnCount();
                for (int i = 1; i <= col; i++)
                {
                    String col_name = md.getColumnName(i);
                    String value = rs.getString(col_name);
                    values.put(col_name, new ObjectByteIterator(value.getBytes()));
                }
                result.add(values);
            }
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
    public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode)
    {
        int retVal = SUCCESS;
        if (profileOwnerID < 0)
        {
            return -1;
        }
        try
        {
            query = "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users u, pendfriendship f WHERE inviteeid=? and f.inviterid = u.userid";
            internalCreateAndCacheStatement(GETPENDINGFRIENDS_STATEMENT, query);
            preparedStatement.setInt(1, profileOwnerID);
            rs = preparedStatement.executeQuery();
            while (rs.next())
            {
                HashMap<String, ByteIterator> values = new HashMap<>();
                String inviterid = "";
                //get the number of columns and their names
                ResultSetMetaData md = rs.getMetaData();
                int columnCount = md.getColumnCount();
                for (int i = 1; i <= columnCount; i++)
                {
                    String col_name = md.getColumnName(i);
                    String value = rs.getString(col_name);
                    if (col_name.equals("inviterid"))
                    {
                        inviterid = value;
                    }
                    values.put(col_name, new ObjectByteIterator(value.getBytes()));
                }

                if (insertImage)
                {
                    // Get as a BLOB
                    byte[] allBytesInBlob = blobManager.getBLOB(BlobManager.TPIC, inviterid);
                    values.put(BlobManager.TPIC, new ObjectByteIterator(allBytesInBlob));
                }
                result.add(values);
            }
            preparedStatement.close();
        }
        catch (AzureClientException | SQLException sx)
        {
            retVal = -2;
            sx.printStackTrace(System.out);
        }
        return retVal;
    }

    @Override
    public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode)
    {
        int retVal = SUCCESS;
        if (requesterID < 0 || profileOwnerID < 0)
        {
            return -1;
        }
        //friend count
        query = "SELECT count(*) FROM  conffriendship WHERE inviterID = ?";
        try
        {
            internalCreateAndCacheStatement(GETFRIENDCOUNT_STATEMENT, query);
            preparedStatement.setInt(1, profileOwnerID);
            rs = preparedStatement.executeQuery();
            rs.next();
            result.put("friendcount", new ObjectByteIterator(rs.getString(1).getBytes()));
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            retVal = -2;
            sx.printStackTrace(System.out);
        }
        //pending friend request count
        //if owner viweing her own profile, she can view her pending friend requests
        if (requesterID == profileOwnerID)
        {
            query = "SELECT count(*) FROM  pendfriendship WHERE inviteeID = ?";
            try
            {
                internalCreateAndCacheStatement(GETPENDINGFRIENDCOUNT_STATEMENT, query);
                preparedStatement.setInt(1, profileOwnerID);
                rs = preparedStatement.executeQuery();
                rs.next();
                result.put("pendingcount", new ObjectByteIterator(rs.getString(1).getBytes()));
                preparedStatement.close();
            }
            catch (SQLException sx)
            {
                retVal = -2;
                sx.printStackTrace(System.out);
            }
        }
        //resource count
        query = "SELECT count(*) FROM  resources WHERE wallUserID = ?";
        try
        {
            internalCreateAndCacheStatement(GETRESOURCECOUNT_STATEMENT, query);
            preparedStatement.setInt(1, profileOwnerID);
            rs = preparedStatement.executeQuery();
            rs.next();
            result.put("resourcecount", new ObjectByteIterator(rs.getString(1).getBytes()));
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            retVal = -2;
            sx.printStackTrace(System.out);
        }
        //profile details
        try
        {
            query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  users WHERE UserID = ?";
            internalCreateAndCacheStatement(GETPROFILE_STATEMENT, query);
            preparedStatement.setInt(1, profileOwnerID);
            rs = preparedStatement.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            int col = md.getColumnCount();
            rs.next();
            for (int i = 1; i <= col; i++)
            {
                String col_name = md.getColumnName(i);
                String value = rs.getString(col_name);
                result.put(col_name, new ObjectByteIterator(value.getBytes()));
            }
            if (insertImage)
            {
                byte[] allBytesInBlob = blobManager.getBLOB(BlobManager.PIC, Integer.toString(profileOwnerID));
                result.put(BlobManager.PIC, new ObjectByteIterator(allBytesInBlob));
            }
            preparedStatement.close();
        }
        catch (AzureClientException | SQLException sx)
        {
            retVal = -2;
            sx.printStackTrace(System.out);
        }
        return retVal;
    }

    @Override
    public int viewTopKResources(int requesterID, int profileOwnerID, int k, Vector<HashMap<String, ByteIterator>> result)
    {
        int retVal = SUCCESS;
        if (profileOwnerID < 0 || requesterID < 0 || k < 0)
        {
            return -1;
        }
        query = "SELECT TOP " + k + " * FROM resources WHERE walluserid = ? ORDER BY rid desc";
        try
        {
            internalCreateAndCacheStatement(GETTOPRESOURCES_STATEMENT, query);
            preparedStatement.setInt(1, profileOwnerID);
            rs = preparedStatement.executeQuery();
            while (rs.next())
            {
                HashMap<String, ByteIterator> values = new HashMap<>();
                //get the number of columns and their names
                ResultSetMetaData metaData = rs.getMetaData();
                int col = metaData.getColumnCount();
                for (int i = 1; i <= col; i++)
                {
                    String col_name = metaData.getColumnName(i);
                    String value = rs.getString(col_name);
                    if (col_name.equalsIgnoreCase("rid"))
                    {
                        col_name = "rid";
                    }
                    else if (col_name.equalsIgnoreCase("walluserid"))
                    {
                        col_name = "walluserid";
                    }
                    else if (col_name.equalsIgnoreCase("creatorid"))
                    {
                        col_name = "creatorid";
                    }
                    values.put(col_name, new ObjectByteIterator(value.getBytes()));
                }
                result.add(values);
            }
            preparedStatement.close();
        }
        catch (SQLException sx)
        {
            retVal = -2;
            System.out.println("Error while viewing top k resources!");
        }
        return retVal;
    }
}
