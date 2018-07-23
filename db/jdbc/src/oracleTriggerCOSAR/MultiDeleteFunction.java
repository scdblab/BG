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

package oracleTriggerCOSAR;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class MultiDeleteFunction {

	public static void RegFunctions(String driver, String url, String user, String passwd)
	{
		Connection conn = null;
		Statement st = null;

		try
		{
			Class.forName (driver);

			conn = DriverManager.getConnection(url,user,passwd);
			st = conn.createStatement();


			st.execute("CREATE OR REPLACE LIBRARY COSAR_DLL_LIB AS \'C:\\cosar\\x64\\release\\COSARTriggerAPI.dll\';");


			st.execute(
					"CREATE OR REPLACE FUNCTION COSARDeleteCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARDelete\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE FUNCTION COSARTriggerDeleteCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARTriggerDelete\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE FUNCTION COSARTriggerDeleteMultiCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARTriggerDeleteMulti\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE FUNCTION COSARDeleteMultiCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARDeleteMulti\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);
		} 
		catch(SQLException e )
		{
			e.printStackTrace(System.out);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
	}


	public static void TestSQL(String driver, String url, String username, String passwd)
	{
		Connection conn = null;
		Statement st = null;
		PreparedStatement ps = null;

		try
		{
			Class.forName (driver);
			conn = DriverManager.getConnection(url,username,passwd);
			st = conn.createStatement();
			ps = conn.prepareStatement("SELECT id, creation_date FROM root.users WHERE nickname=?");

			//st.execute("");
			ps.setString(1, "user854792");
			//ps.executeQuery("SELECT id FROM root.regions WHERE name='OR--Portland'");
			ResultSet rs = ps.executeQuery();
			if( rs.next() )
			{
				String date = rs.getString("creation_date");
			}

		} 
		catch(SQLException e )
		{
			e.printStackTrace(System.out);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
	}

	public static void RegTrigMultiDelete(String driver, String url, String username, String passwd)
	{

		Connection conn = null;
		Statement st = null;

		try
		{
			Class.forName (driver);
			conn = DriverManager.getConnection(url,username,passwd);
			st = conn.createStatement();


			st.execute("CREATE OR REPLACE LIBRARY COSAR_DLL_LIB AS \'C:\\cosar\\x64\\release\\COSARTriggerAPI.dll\';");


			st.execute(
					"CREATE OR REPLACE FUNCTION COSARDeleteCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARDelete\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE FUNCTION COSARTriggerDeleteCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARTriggerDelete\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE FUNCTION COSARTriggerDeleteMultiCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARTriggerDeleteMulti\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE FUNCTION COSARDeleteMultiCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARDeleteMulti\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE PROCEDURE invalidateFriendCacheEntry ( "+
							"			user_id IN NUMBER, "+
							"			recursion IN NUMBER, DELETEKEY IN OUT CLOB ) "+
							" IS "+
							"  		ffid friends.friendid%TYPE; " +
							"  		fuid friends.userid%TYPE; " +
							"       ret_val BINARY_INTEGER; " +
							"  		k3  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  		friendprofile  CLOB := TO_CLOB('ProfileJSP'); " +
							"  		ids CLOB; " +
							"  		CURSOR FriendCursor IS " +
							"			select userid, friendid " +
							"			from friends " +
							"			where status='2' and " +
							"(userid=user_id OR " +
							"friendid=user_id); " +
							" BEGIN "+
							//" 	dbms_output.put_line('COSAR DELETE ' || CONCAT(friendprofile,user_id) ); "+
							//"   ret_val := COSARDeleteCall('RAYS',CONCAT(friendprofile,user_id), 0); "+
							" DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', CONCAT(friendprofile,user_id)));  "+
							"  	OPEN FriendCursor; " +
							"  	LOOP " +
							"     FETCH FriendCursor INTO fuid, ffid; " +
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"    IF ffid != user_id THEN " +
							"		ids := CONCAT(ffid,'-'); " + 
							"		ids := CONCAT(ids,user_id); " +		
							" DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', CONCAT(k3,ids)));  "+
							//"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k3,ids),0); " +
							//"		dbms_output.put_line('InvalidateFriendCacheEntry recursion = ' || recursion || ': k3 = ' || CONCAT(k3,ids) ); " +
							"       IF (recursion!=1) THEN invalidateFriendCacheEntry(ffid, 1, DELETEKEY); END IF; " +
							"     END IF; " +
							"    IF fuid != user_id THEN " + 
							"		ids := CONCAT(fuid,'-'); " +
							"		ids := CONCAT(ids,user_id); " +	
							" DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', CONCAT(k3,ids)));  "+
							//"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k3,ids),0); " +
							//"		dbms_output.put_line('InvalidateFriendCacheEntry recursion = ' || recursion || 'k3 = ' || CONCAT(k3,ids) ); " + 
							"      IF (recursion!=1) THEN invalidateFriendCacheEntry(fuid, 1, DELETEKEY); END IF; " +
							"    END IF; 	" +
							" END LOOP; " +
							" CLOSE FriendCursor; " + 
							" END; "
					);


			/*
				st.execute(		
						"CREATE OR REPLACE TRIGGER COSAR_DLL_TRIGGER " +
						"AFTER UPDATE ON cosar.test_table " +
						"DECLARE " +
						"	func_ret_val BINARY_INTEGER; " +
						"BEGIN " +
						"	func_ret_val := COSARDeleteCall(\'testkey1\',\'testValue1\',0); " +
						"   dbms_output.put_line('func_ret_val = ' || func_ret_val); " +
						"END; "
				);
				//*/

			st.execute(
					"CREATE OR REPLACE TRIGGER StreamStatusChg3 " +
							"BEFORE UPDATE ON user_cameras " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('CameraTabJSP'); " +
							"  k2  CLOB := TO_CLOB('ProfileUserInformationJSP'); " +
							"  k3  CLOB := TO_CLOB('LoadLivestreamsJSP'); " +
							//"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							//"  k5  CLOB := TO_CLOB('ProfileJSP'); " +
							"  DELETEKEY CLOB; " +
							"  ids CLOB; " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :NEW.user_id; " +
							/* dbms_output.put_line('Trigger is before; uid = ' || uid); */
							"  k1 := CONCAT(k1,uid); " +
							"  k2 := CONCAT(k2,uid); " +
							"  k3 := CONCAT(k3,uid); " +
							" DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k1));  "+
							" DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k2));  "+
							" DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+

						//"  ret_val := COSARDeleteCall('RAYS',k1,0); " +
						//"  dbms_output.put_line('k1 delete ret_val=' || ret_val); " +
						//"  ret_val := COSARDeleteCall('RAYS',k2,0); " +
						//"  dbms_output.put_line('k2 delete ret_val=' || ret_val); " +
						//"  ret_val := COSARDeleteCall('RAYS',k3,0); " +
						//"  dbms_output.put_line('k1 = ' || k1 || ', k2 = ' || k2 || ', k3 = ' || k3); " +  
						"  invalidateFriendCacheEntry(uid, 0, DELETEKEY); " + 
						" ret_val := COSARDeleteMultiCall('RAYS', DELETEKEY, 0); "+
						" END; "
					);		

			st.execute(
					"CREATE OR REPLACE TRIGGER UserInfoChangeTrigger " +
							"BEFORE UPDATE OR DELETE ON users " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('ProfileJSP'); " +
							"  k2  CLOB := TO_CLOB('RetrFrndsJSP'); " +					
							"  k3  CLOB := TO_CLOB('LoadLivestreamsJSP'); " +					
							"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  ids CLOB; " +
							//"  k5  CLOB := TO_CLOB('ProfileUserInformationJSP'); " +
							"  ret_val BINARY_INTEGER; " +
							"  ffid friends.friendid%TYPE; " +
							"  fuid friends.userid%TYPE; " +
							"  CURSOR FriendCursor IS " +
							"      select friendid, userid from friends " +
							"      where status='2' AND ( userid=uid OR friendid=uid ); " +
							"BEGIN " +
							"  uid := :OLD.user_id; " +
							/* dbms_output.put_line('Trigger is before; uid = ' || uid); */
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,uid),0); " +
							"  dbms_output.put_line('k2 delete ret_val=' || ret_val); " +
							"  OPEN FriendCursor; " +
							"  LOOP " +
							"     FETCH FriendCursor INTO ffid, fuid; " +
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"     IF ffid != uid THEN " +
							"		ids := CONCAT(uid,'-'); " +
							"		ids := CONCAT(ids,ffid); " +		
							//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +
							"     IF fuid != uid THEN " +
							"		ids := CONCAT(fuid,'-'); " +
							"		ids := CONCAT(ids,uid); " +		
							//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +					
							"  END LOOP; " +
							"  CLOSE FriendCursor; " +
							"END; "
					);


			st.execute(
					"CREATE OR REPLACE TRIGGER UserInfoInsertTrigger " +
							"BEFORE INSERT ON users " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('ProfileJSP'); " +
							"  k2  CLOB := TO_CLOB('RetrFrndsJSP'); " +					
							"  k3  CLOB := TO_CLOB('LoadLivestreamsJSP'); " +					
							"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  ids CLOB; " +
							//"  k5  CLOB := TO_CLOB('ProfileUserInformationJSP'); " +
							"  ret_val BINARY_INTEGER; " +
							"  ffid friends.friendid%TYPE; " +
							"  fuid friends.userid%TYPE; " +
							"  CURSOR FriendCursor IS " +
							"      select friendid, userid from friends " +
							"      where status='2' AND ( userid=uid OR friendid=uid ); " +
							"BEGIN " +
							"  uid := :NEW.user_id; " +
							/* dbms_output.put_line('Trigger is before; uid = ' || uid); */
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,uid),0); " +
							"  dbms_output.put_line('k2 delete ret_val=' || ret_val); " +
							"  OPEN FriendCursor; " +
							"  LOOP " +
							"     FETCH FriendCursor INTO ffid, fuid; " +
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"     IF ffid != uid THEN " +
							"		ids := CONCAT(uid,'-'); " +
							"		ids := CONCAT(ids,ffid); " +		
							//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +
							"     IF fuid != uid THEN " +
							"		ids := CONCAT(fuid,'-'); " +
							"		ids := CONCAT(ids,uid); " +		
							//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +					
							"  END LOOP; " +
							"  CLOSE FriendCursor; " +
							"END; "
					);

			st.execute(
					"CREATE OR REPLACE TRIGGER FriendshipChangeTrigger " +
							"BEFORE UPDATE OR DELETE ON friends " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  fid number; " +
							"  k1  CLOB := TO_CLOB('ProfileJSP'); " +
							"  k2  CLOB := TO_CLOB('RetrFrndsJSP'); " +					
							"  k3  CLOB := TO_CLOB('LoadRecentRecomVideosJSP'); " +					
							"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  ids CLOB; " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :OLD.userid; " +
							"  fid := :OLD.friendid; " +
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,fid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,fid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,fid),0); " +
							"  ids := CONCAT( uid, '-' ); " +
							"  ids := CONCAT( ids, fid ); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"  ids := CONCAT( fid, '-' ); " +
							"  ids := CONCAT( ids, uid ); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"  dbms_output.put_line('delete ret_val=' || ret_val); " +					
							"END; "
					);


			st.execute(
					"CREATE OR REPLACE TRIGGER FriendshipInsertTrigger " +
							"BEFORE INSERT ON friends " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  fid number; " +
							"  k1  CLOB := TO_CLOB('ProfileJSP'); " +
							"  k2  CLOB := TO_CLOB('RetrFrndsJSP'); " +					
							"  k3  CLOB := TO_CLOB('LoadRecentRecomVideosJSP'); " +					
							"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  ids CLOB; " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :NEW.userid; " +
							"  fid := :NEW.friendid; " +
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,fid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,fid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,fid),0); " +
							"  ids := CONCAT( uid, '-' ); " +
							"  ids := CONCAT( ids, fid ); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"  ids := CONCAT( fid, '-' ); " +
							"  ids := CONCAT( ids, uid ); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"  dbms_output.put_line('delete ret_val=' || ret_val); " +					
							"END; "
					);


			st.execute(
					"CREATE OR REPLACE TRIGGER RecommendedVideoInsertTrigger " +
							"BEFORE INSERT ON recommended_videos " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('LoadRecentRecomVideosJSP'); " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :NEW.recommended_to; " +
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  dbms_output.put_line('delete ret_val=' || ret_val); " +					
							"END; "
					);

			st.execute(
					"CREATE OR REPLACE TRIGGER RecommendedVideoChangeTrigger " +
							"BEFORE UPDATE OR DELETE ON recommended_videos " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('LoadRecentRecomVideosJSP'); " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :OLD.recommended_to; " +
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  dbms_output.put_line('delete ret_val=' || ret_val); " +					
							"END; "
					);

			st.execute(
					"CREATE OR REPLACE TRIGGER userCameraCHGTrigger " + 
							"BEFORE DELETE OR UPDATE ON user_cameras " +
							"FOR EACH ROW " +
							"DECLARE " +
							"uid number := 5; " +
							"k1  CLOB := TO_CLOB('CameraTabJSP'); " +
							"ret_val BINARY_INTEGER;  " +
							"BEGIN " +
							"	uid := :OLD.user_id; " +
							//"	dbms_output.put_line('userCameraCHGTrigger replace this printout with the cosar delete call for key' || CONCAT(k1,uid));
							"	ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"END;"		
					);

			st.execute(
					"CREATE OR REPLACE TRIGGER msgDelUpdTrigger " +
							"BEFORE DELETE OR UPDATE ON messageinbox " +
							"FOR EACH ROW " +
							"DECLARE " +
							"uid number := 5; " +
							"k1  CLOB := TO_CLOB('ProfileJSP');  " +
							"k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"ids CLOB; " +
							"ret_val BINARY_INTEGER;  " +
							"ffid friends.friendid%TYPE;  " +
							"fuid friends.userid%TYPE;  " +
							"CURSOR FriendCursor IS  " +
							"      select friendid, userid from friends  " +
							"      where status='2' AND ( userid=uid OR friendid=uid ); " +  
							"BEGIN " +
							"uid := :OLD.receiver; " +
							//"dbms_output.put_line('msgTrigger replace this printout with the cosar delete call for key' || CONCAT(k1,uid));
							"ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"LOOP " + 
							"     FETCH FriendCursor INTO ffid, fuid; " + 
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"     IF ffid != uid THEN " + 
							"               ids := CONCAT(uid,'-'); " + 
							"               ids := CONCAT(ids,ffid); " + 
							//"               dbms_output.put_line('msgTrigger 1 replace with cosar delete call for k4 = ' || CONCAT(k4,ids) ); 
							"               ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0);  " +
							"     END IF; " + 
							"     IF fuid != uid THEN " +
							"               ids := CONCAT(fuid,'-'); " +
							"               ids := CONCAT(ids,uid); " + 
							//"               dbms_output.put_line('msgTrigger 2.1 replace with cosar delete call for k4 = ' || CONCAT(k4,ids) ); 
							"       		ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +
							"  END LOOP; " + 
							"  CLOSE FriendCursor; " + 
							"END; "
					);


			st.execute(
					"CREATE OR REPLACE TRIGGER msgInsertTrigger " +
							"BEFORE INSERT ON messageinbox " +
							"FOR EACH ROW " +
							"DECLARE " +
							"uid number := 5; " +
							"k1  CLOB := TO_CLOB('ProfileJSP');  " +
							"k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"ids CLOB; " +
							"ret_val BINARY_INTEGER;  " +
							"ffid friends.friendid%TYPE;  " +
							"fuid friends.userid%TYPE;  " +
							"CURSOR FriendCursor IS  " +
							"      select friendid, userid from friends  " +
							"      where status='2' AND ( userid=uid OR friendid=uid ); " +  
							"BEGIN " +
							"uid := :NEW.receiver; " +
							//"dbms_output.put_line('msgTrigger replace this printout with the cosar delete call for key' || CONCAT(k1,uid));
							"ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"LOOP " + 
							"     FETCH FriendCursor INTO ffid, fuid; " + 
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"     IF ffid != uid THEN " + 
							"               ids := CONCAT(uid,'-'); " + 
							"               ids := CONCAT(ids,ffid); " + 
							//"               dbms_output.put_line('msgTrigger 1 replace with cosar delete call for k4 = ' || CONCAT(k4,ids) ); 
							"               ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0);  " +
							"     END IF; " + 
							"     IF fuid != uid THEN " +
							"               ids := CONCAT(fuid,'-'); " +
							"               ids := CONCAT(ids,uid); " + 
							//"               dbms_output.put_line('msgTrigger 2.1 replace with cosar delete call for k4 = ' || CONCAT(k4,ids) ); 
							"       		ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +
							"  END LOOP; " + 
							"  CLOSE FriendCursor; " + 
							"END; "
					);


		}
		catch( Exception e )
		{
			e.printStackTrace(System.out);
		}	
		finally
		{
			try
			{
				if( st != null )
				{
					st.close();
				}
				if( conn != null )
				{
					conn.close();				
				}
			}
			catch( Exception e )
			{
				e.printStackTrace(System.out);
			}			
		}
	}

	public static void RegTrigSingleDelete(String driver, String url, String username, String passwd)
	{

		Connection conn = null;
		Statement st = null;

		try
		{
			Class.forName (driver);
			conn = DriverManager.getConnection(url,username,passwd);
			st = conn.createStatement();


			st.execute("CREATE OR REPLACE LIBRARY COSAR_DLL_LIB AS \'C:\\cosar\\x64\\release\\COSARTriggerAPI.dll\';");


			st.execute(
					"CREATE OR REPLACE FUNCTION COSARDeleteCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARDelete\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE FUNCTION COSARTriggerDeleteCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARTriggerDelete\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE FUNCTION COSARTriggerDeleteMultiCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARTriggerDeleteMulti\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);


			st.execute(
					"CREATE OR REPLACE FUNCTION COSARDeleteMultiCall  ( " +
							"		   datazone   IN VARCHAR2, " +
							"		   cosar_key  IN VARCHAR2, " +
							"		   flags	  IN BINARY_INTEGER ) "+
							"		RETURN BINARY_INTEGER " +
							"		AS LANGUAGE C "+
							"		   LIBRARY COSAR_DLL_LIB  "+
							"		   NAME \"COSARDeleteMulti\"  "+
							"		   PARAMETERS ( "+
							"		      datazone, "+
							"		      datazone LENGTH, "+
							"		      cosar_key, "+
							"		      cosar_key LENGTH, "+
							"			  flags int); "
					);

			st.execute(
					"CREATE OR REPLACE PROCEDURE invalidateFriendCacheEntry ( "+
							"			user_id IN NUMBER, "+
							"			recursion IN NUMBER ) "+
							" IS "+
							"  		ffid friends.friendid%TYPE; " +
							"  		fuid friends.userid%TYPE; " +
							"       ret_val BINARY_INTEGER; " +
							"  		k3  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  		friendprofile  CLOB := TO_CLOB('ProfileJSP'); " +
							"  		ids CLOB; " +
							"  		CURSOR FriendCursor IS " +
							"			select userid, friendid " +
							"			from friends " +
							"			where status='2' and " +
							"(userid=user_id OR " +
							"friendid=user_id); " +
							" BEGIN "+
							//" 	dbms_output.put_line('COSAR DELETE ' || CONCAT(friendprofile,user_id) ); "+
							"   ret_val := COSARDeleteCall('RAYS',CONCAT(friendprofile,user_id), 0); "+
							"  	OPEN FriendCursor; " +
							"  	LOOP " +
							"     FETCH FriendCursor INTO fuid, ffid; " +
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"    IF ffid != user_id THEN " +
							"		ids := CONCAT(ffid,'-'); " + 
							"		ids := CONCAT(ids,user_id); " +		
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k3,ids),0); " +
							//"		dbms_output.put_line('InvalidateFriendCacheEntry recursion = ' || recursion || ': k3 = ' || CONCAT(k3,ids) ); " +
							"       IF (recursion!=1) THEN invalidateFriendCacheEntry(ffid, 1); END IF; " +
							"     END IF; " +
							"    IF fuid != user_id THEN " + 
							"		ids := CONCAT(fuid,'-'); " +
							"		ids := CONCAT(ids,user_id); " +		
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k3,ids),0); " +
							//"		dbms_output.put_line('InvalidateFriendCacheEntry recursion = ' || recursion || 'k3 = ' || CONCAT(k3,ids) ); " + 
							"      IF (recursion!=1) THEN invalidateFriendCacheEntry(fuid, 1); END IF; " +
							"    END IF; 	" +
							" END LOOP; " +
							" CLOSE FriendCursor; " + 
							" END; "
					);


			/*
				st.execute(		
						"CREATE OR REPLACE TRIGGER COSAR_DLL_TRIGGER " +
						"AFTER UPDATE ON cosar.test_table " +
						"DECLARE " +
						"	func_ret_val BINARY_INTEGER; " +
						"BEGIN " +
						"	func_ret_val := COSARDeleteCall(\'testkey1\',\'testValue1\',0); " +
						"   dbms_output.put_line('func_ret_val = ' || func_ret_val); " +
						"END; "
				);
				//*/

			st.execute(
					"CREATE OR REPLACE TRIGGER StreamStatusChg3 " +
							"BEFORE UPDATE ON user_cameras " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('CameraTabJSP'); " +
							"  k2  CLOB := TO_CLOB('ProfileUserInformationJSP'); " +
							"  k3  CLOB := TO_CLOB('LoadLivestreamsJSP'); " +
							"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  k5  CLOB := TO_CLOB('ProfileJSP'); " +
							"  ids CLOB; " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :NEW.user_id; " +
							/* dbms_output.put_line('Trigger is before; uid = ' || uid); */
							"  k1 := CONCAT(k1,uid); " +
							"  k2 := CONCAT(k2,uid); " +
							"  k3 := CONCAT(k3,uid); " +
							"  ret_val := COSARDeleteCall('RAYS',k1,0); " +
							//"  dbms_output.put_line('k1 delete ret_val=' || ret_val); " +
							"  ret_val := COSARDeleteCall('RAYS',k2,0); " +
							//"  dbms_output.put_line('k2 delete ret_val=' || ret_val); " +
							"  ret_val := COSARDeleteCall('RAYS',k3,0); " +
							//"  dbms_output.put_line('k1 = ' || k1 || ', k2 = ' || k2 || ', k3 = ' || k3); " +  
							"  invalidateFriendCacheEntry(uid, 0); " + 
							" END; "
					);

			//				st.execute(
			//						"CREATE OR REPLACE TRIGGER StreamStatusChg3 " +
			//						"BEFORE UPDATE ON user_cameras " +
			//						"FOR EACH ROW " +
			//						"DECLARE " +
			//						"  uid number := 5; " +
			//						"  k1  CLOB := TO_CLOB('CameraTabJSP'); " +
			//						"  k2  CLOB := TO_CLOB('ProfileUserInformationJSP'); " +
			//						"  k3  CLOB := TO_CLOB('LoadLivestreamsJSP'); " +
			//						"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
			//						"  k5  CLOB := TO_CLOB('ProfileJSP'); " +
			//						"  ids CLOB; " +
			//						"  ret_val BINARY_INTEGER; " +
			//						"  ffid friends.friendid%TYPE; " +
			//						"  fuid friends.userid%TYPE; " +
			//						"  CURSOR FriendCursor IS " +
			//						"	   select friendid, userid from friends " +
			//						"	   where userid in " +
			//						"		(select friendid " +
			//						"		from friends " +
			//						"		where status='2' AND ( userid=uid OR friendid=uid )) " +
			//						"	   OR friendid in " +
			//						"		(select userid " +
			//						"		from friends " +
			//						"		where status='2' AND ( userid=uid OR friendid=uid )); " +
			//						"BEGIN " +
			//						"  uid := :NEW.user_id; " +
			//						  /* dbms_output.put_line('Trigger is before; uid = ' || uid); */
			//						"  k1 := CONCAT(k1,uid); " +
			//						"  k2 := CONCAT(k2,uid); " +
			//						"  k3 := CONCAT(k3,uid); " +
			//						"  dbms_output.put_line('k1 = ' || k1 || ', k2 = ' || k2); " +
			//						"  ret_val := COSARDeleteCall('RAYS',k1,0); " +
			//						//"  dbms_output.put_line('k1 delete ret_val=' || ret_val); " +
			//						"  ret_val := COSARDeleteCall('RAYS',k2,0); " +
			//						//"  dbms_output.put_line('k2 delete ret_val=' || ret_val); " +
			//						"  ret_val := COSARDeleteCall('RAYS',k3,0); " +
			//						"  OPEN FriendCursor; " +
			//						"  LOOP " +
			//						"     FETCH FriendCursor INTO ffid, fuid; " +
			//						"     EXIT WHEN FriendCursor%NOTFOUND; " +
			//						"	  ids := CONCAT(fuid, '-'); " +
			//						"	  ids := CONCAT(ids, ffid); " +
			//						"     ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
			//						"	  ids := CONCAT(ffid, '-'); " +
			//						"	  ids := CONCAT(ids, fuid); " +
			//						"     ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
			//						"     IF ffid = uid THEN " +
			//						"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k5,fuid),0); " +
			//						"	  END IF; " +
			//						"     IF fuid = uid THEN " +
			//						"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k5,ffid),0); " +
			//						"	  END IF; " +
			////						"     IF ffid != uid THEN " +
			////						"		ids := CONCAT(uid,'-'); " +
			////						"		ids := CONCAT(ids,ffid); " +		
			////						//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
			////						"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
			////						"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k5,ffid),0); " +
			////						"     END IF; " +
			////						"     IF fuid != uid THEN " +
			////						"		ids := CONCAT(fuid,'-'); " +
			////						"		ids := CONCAT(ids,uid); " +		
			////						//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
			////						"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
			////						"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k5,fuid),0); " +
			////						"     END IF; " +					
			//						"  END LOOP; " +
			//						"  CLOSE FriendCursor; " +
			//						"END; "
			//				);
			//				

			st.execute(
					"CREATE OR REPLACE TRIGGER UserInfoChangeTrigger " +
							"BEFORE UPDATE OR DELETE ON users " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('ProfileJSP'); " +
							"  k2  CLOB := TO_CLOB('RetrFrndsJSP'); " +					
							"  k3  CLOB := TO_CLOB('LoadLivestreamsJSP'); " +					
							"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  ids CLOB; " +
							//"  k5  CLOB := TO_CLOB('ProfileUserInformationJSP'); " +
							"  ret_val BINARY_INTEGER; " +
							"  ffid friends.friendid%TYPE; " +
							"  fuid friends.userid%TYPE; " +
							"  CURSOR FriendCursor IS " +
							"      select friendid, userid from friends " +
							"      where status='2' AND ( userid=uid OR friendid=uid ); " +
							"BEGIN " +
							"  uid := :OLD.user_id; " +
							/* dbms_output.put_line('Trigger is before; uid = ' || uid); */
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,uid),0); " +
							"  dbms_output.put_line('k2 delete ret_val=' || ret_val); " +
							"  OPEN FriendCursor; " +
							"  LOOP " +
							"     FETCH FriendCursor INTO ffid, fuid; " +
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"     IF ffid != uid THEN " +
							"		ids := CONCAT(uid,'-'); " +
							"		ids := CONCAT(ids,ffid); " +		
							//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +
							"     IF fuid != uid THEN " +
							"		ids := CONCAT(fuid,'-'); " +
							"		ids := CONCAT(ids,uid); " +		
							//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +					
							"  END LOOP; " +
							"  CLOSE FriendCursor; " +
							"END; "
					);


			st.execute(
					"CREATE OR REPLACE TRIGGER UserInfoInsertTrigger " +
							"BEFORE INSERT ON users " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('ProfileJSP'); " +
							"  k2  CLOB := TO_CLOB('RetrFrndsJSP'); " +					
							"  k3  CLOB := TO_CLOB('LoadLivestreamsJSP'); " +					
							"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  ids CLOB; " +
							//"  k5  CLOB := TO_CLOB('ProfileUserInformationJSP'); " +
							"  ret_val BINARY_INTEGER; " +
							"  ffid friends.friendid%TYPE; " +
							"  fuid friends.userid%TYPE; " +
							"  CURSOR FriendCursor IS " +
							"      select friendid, userid from friends " +
							"      where status='2' AND ( userid=uid OR friendid=uid ); " +
							"BEGIN " +
							"  uid := :NEW.user_id; " +
							/* dbms_output.put_line('Trigger is before; uid = ' || uid); */
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,uid),0); " +
							"  dbms_output.put_line('k2 delete ret_val=' || ret_val); " +
							"  OPEN FriendCursor; " +
							"  LOOP " +
							"     FETCH FriendCursor INTO ffid, fuid; " +
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"     IF ffid != uid THEN " +
							"		ids := CONCAT(uid,'-'); " +
							"		ids := CONCAT(ids,ffid); " +		
							//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +
							"     IF fuid != uid THEN " +
							"		ids := CONCAT(fuid,'-'); " +
							"		ids := CONCAT(ids,uid); " +		
							//"		dbms_output.put_line('k4 = ' || CONCAT(k4,ids) ); " +
							"     	ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +					
							"  END LOOP; " +
							"  CLOSE FriendCursor; " +
							"END; "
					);

			st.execute(
					"CREATE OR REPLACE TRIGGER FriendshipChangeTrigger " +
							"BEFORE UPDATE OR DELETE ON friends " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  fid number; " +
							"  k1  CLOB := TO_CLOB('ProfileJSP'); " +
							"  k2  CLOB := TO_CLOB('RetrFrndsJSP'); " +					
							"  k3  CLOB := TO_CLOB('LoadRecentRecomVideosJSP'); " +					
							"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  ids CLOB; " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :OLD.userid; " +
							"  fid := :OLD.friendid; " +
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,fid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,fid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,fid),0); " +
							"  ids := CONCAT( uid, '-' ); " +
							"  ids := CONCAT( ids, fid ); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"  ids := CONCAT( fid, '-' ); " +
							"  ids := CONCAT( ids, uid ); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"  dbms_output.put_line('delete ret_val=' || ret_val); " +					
							"END; "
					);


			st.execute(
					"CREATE OR REPLACE TRIGGER FriendshipInsertTrigger " +
							"BEFORE INSERT ON friends " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  fid number; " +
							"  k1  CLOB := TO_CLOB('ProfileJSP'); " +
							"  k2  CLOB := TO_CLOB('RetrFrndsJSP'); " +					
							"  k3  CLOB := TO_CLOB('LoadRecentRecomVideosJSP'); " +					
							"  k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"  ids CLOB; " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :NEW.userid; " +
							"  fid := :NEW.friendid; " +
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,fid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k2,fid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,uid),0); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k3,fid),0); " +
							"  ids := CONCAT( uid, '-' ); " +
							"  ids := CONCAT( ids, fid ); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"  ids := CONCAT( fid, '-' ); " +
							"  ids := CONCAT( ids, uid ); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"  dbms_output.put_line('delete ret_val=' || ret_val); " +					
							"END; "
					);


			st.execute(
					"CREATE OR REPLACE TRIGGER RecommendedVideoInsertTrigger " +
							"BEFORE INSERT ON recommended_videos " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('LoadRecentRecomVideosJSP'); " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :NEW.recommended_to; " +
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  dbms_output.put_line('delete ret_val=' || ret_val); " +					
							"END; "
					);

			st.execute(
					"CREATE OR REPLACE TRIGGER RecommendedVideoChangeTrigger " +
							"BEFORE UPDATE OR DELETE ON recommended_videos " +
							"FOR EACH ROW " +
							"DECLARE " +
							"  uid number := 5; " +
							"  k1  CLOB := TO_CLOB('LoadRecentRecomVideosJSP'); " +
							"  ret_val BINARY_INTEGER; " +
							"BEGIN " +
							"  uid := :OLD.recommended_to; " +
							"  dbms_output.put_line('uid = ' || uid); " +
							"  ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"  dbms_output.put_line('delete ret_val=' || ret_val); " +					
							"END; "
					);

			st.execute(
					"CREATE OR REPLACE TRIGGER userCameraCHGTrigger " + 
							"BEFORE DELETE OR UPDATE ON user_cameras " +
							"FOR EACH ROW " +
							"DECLARE " +
							"uid number := 5; " +
							"k1  CLOB := TO_CLOB('CameraTabJSP'); " +
							"ret_val BINARY_INTEGER;  " +
							"BEGIN " +
							"	uid := :OLD.user_id; " +
							//"	dbms_output.put_line('userCameraCHGTrigger replace this printout with the cosar delete call for key' || CONCAT(k1,uid));
							"	ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"END;"		
					);

			st.execute(
					"CREATE OR REPLACE TRIGGER msgDelUpdTrigger " +
							"BEFORE DELETE OR UPDATE ON messageinbox " +
							"FOR EACH ROW " +
							"DECLARE " +
							"uid number := 5; " +
							"k1  CLOB := TO_CLOB('ProfileJSP');  " +
							"k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"ids CLOB; " +
							"ret_val BINARY_INTEGER;  " +
							"ffid friends.friendid%TYPE;  " +
							"fuid friends.userid%TYPE;  " +
							"CURSOR FriendCursor IS  " +
							"      select friendid, userid from friends  " +
							"      where status='2' AND ( userid=uid OR friendid=uid ); " +  
							"BEGIN " +
							"uid := :OLD.receiver; " +
							//"dbms_output.put_line('msgTrigger replace this printout with the cosar delete call for key' || CONCAT(k1,uid));
							"ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"LOOP " + 
							"     FETCH FriendCursor INTO ffid, fuid; " + 
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"     IF ffid != uid THEN " + 
							"               ids := CONCAT(uid,'-'); " + 
							"               ids := CONCAT(ids,ffid); " + 
							//"               dbms_output.put_line('msgTrigger 1 replace with cosar delete call for k4 = ' || CONCAT(k4,ids) ); 
							"               ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0);  " +
							"     END IF; " + 
							"     IF fuid != uid THEN " +
							"               ids := CONCAT(fuid,'-'); " +
							"               ids := CONCAT(ids,uid); " + 
							//"               dbms_output.put_line('msgTrigger 2.1 replace with cosar delete call for k4 = ' || CONCAT(k4,ids) ); 
							"       		ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +
							"  END LOOP; " + 
							"  CLOSE FriendCursor; " + 
							"END; "
					);


			st.execute(
					"CREATE OR REPLACE TRIGGER msgInsertTrigger " +
							"BEFORE INSERT ON messageinbox " +
							"FOR EACH ROW " +
							"DECLARE " +
							"uid number := 5; " +
							"k1  CLOB := TO_CLOB('ProfileJSP');  " +
							"k4  CLOB := TO_CLOB('FriendProfileJSP'); " +
							"ids CLOB; " +
							"ret_val BINARY_INTEGER;  " +
							"ffid friends.friendid%TYPE;  " +
							"fuid friends.userid%TYPE;  " +
							"CURSOR FriendCursor IS  " +
							"      select friendid, userid from friends  " +
							"      where status='2' AND ( userid=uid OR friendid=uid ); " +  
							"BEGIN " +
							"uid := :NEW.receiver; " +
							//"dbms_output.put_line('msgTrigger replace this printout with the cosar delete call for key' || CONCAT(k1,uid));
							"ret_val := COSARDeleteCall('RAYS',CONCAT(k1,uid),0); " +
							"LOOP " + 
							"     FETCH FriendCursor INTO ffid, fuid; " + 
							"     EXIT WHEN FriendCursor%NOTFOUND; " +
							"     IF ffid != uid THEN " + 
							"               ids := CONCAT(uid,'-'); " + 
							"               ids := CONCAT(ids,ffid); " + 
							//"               dbms_output.put_line('msgTrigger 1 replace with cosar delete call for k4 = ' || CONCAT(k4,ids) ); 
							"               ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0);  " +
							"     END IF; " + 
							"     IF fuid != uid THEN " +
							"               ids := CONCAT(fuid,'-'); " +
							"               ids := CONCAT(ids,uid); " + 
							//"               dbms_output.put_line('msgTrigger 2.1 replace with cosar delete call for k4 = ' || CONCAT(k4,ids) ); 
							"       		ret_val := COSARDeleteCall('RAYS',CONCAT(k4,ids),0); " +
							"     END IF; " +
							"  END LOOP; " + 
							"  CLOSE FriendCursor; " + 
							"END; "
					);


		}
		catch( Exception e )
		{
			e.printStackTrace(System.out);
		}	
		finally
		{
			try
			{
				if( st != null )
				{
					st.close();
				}
				if( conn != null )
				{
					conn.close();				
				}
			}
			catch( Exception e )
			{
				e.printStackTrace(System.out);
			}			
		}
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//RegTrigSingleDelete();
		//RegTrigMultiDelete();
		//RegFunctions();
		System.out.println("Error in MultiDeleteFunction:  Main is currently disabled.");
		//TestSQL();
	}

}
