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

package common;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.Client;
import edu.usc.bg.base.ObjectByteIterator;

public class RdbmsUtilities {

	private static final boolean verbose = false;
	public static boolean USE_RANDOM_BYTES_FOR_IMAGE = true;
	private static Semaphore imgctrl = new Semaphore(10, true);
	
	public static boolean StoreImageInFS(String userid, byte[] image, boolean profileimg, String FSimagePath){
		boolean result = true;
		String ext = "thumbnail";
		
		if (profileimg) ext = "profile";
		
		String ImageFileName = FSimagePath+"\\img"+userid+ext;
		
		File tgt = new File(ImageFileName);
		if ( tgt.exists() ){
			if (! tgt.delete() ) {
				System.out.println("Error, file exists and failed to delete");
				return false;
			}
		}

		//Write the file
		try {
			imgctrl.acquire();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try{
			FileOutputStream fos = new FileOutputStream(ImageFileName);
			fos.write(image);
			fos.close();
		}catch(Exception ex){
			System.out.println("Error in writing the file"+ImageFileName);
			ex.printStackTrace(System.out);
		}
		imgctrl.release();

		return result;
	}
	
	public static byte[] GetImageFromFS(String userid, boolean profileimg, String FSimagePath){
        int filelength = 0;
        String ext = "thumbnail";
        byte[] imgpayload = null;
        BufferedInputStream bis = null;
       
        if (profileimg) ext = "profile";
       
        String ImageFileName = FSimagePath+"\\img"+userid+ext;
       int attempt = 100;
       while(attempt>0){
	         try {
	        	 imgctrl.acquire();
	        	 FileInputStream fis = null;
	        	 DataInputStream dis = null;
	        	 File fsimage = new File(ImageFileName); 
	             filelength = (int) fsimage.length();
	             imgpayload = new byte[filelength];
	             fis = new FileInputStream(fsimage);
	             dis = new DataInputStream(fis);
	             int read = 0;
	             int numRead = 0;
	             while (read < filelength && (numRead=dis.read(imgpayload, read, filelength - read    ) ) >= 0) {
	                     read = read + numRead;
	             }
	             dis.close();
	             fis.close();
	             imgctrl.release();
	             break;
	         } catch (IOException e) {
	             e.printStackTrace(System.out);
	        	 attempt--;
	        	 imgctrl.release();
	         } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				imgctrl.release();
			}
       }
       return imgpayload;
	}
	
	public static int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage, 
			Connection conn, String FSimagePath) {
		if (entitySet == null) {
			return -1;
		}
		if (entityPK == null) {
			return -1;
		}
		ResultSet rs =null;

		PreparedStatement preparedStatement = null;
		
		try {
			String query;
			int numFields = values.size();
			
			if(entitySet.equalsIgnoreCase("users") && insertImage && !FSimagePath.equals(""))
				numFields = numFields-2;
			query = "INSERT INTO "+entitySet+" VALUES (";
			for(int j=0; j<=numFields; j++){
				if(j==(numFields)){
					query+="?)";
					break;
				}else
					query+="?,";
			}

			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setString(1, entityPK);
			int cnt=2;
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				//blobs need to be inserted differently
				if(entry.getKey().equalsIgnoreCase("pic") || entry.getKey().equalsIgnoreCase("tpic") )
					continue;
				
				String field = entry.getValue().toString();
				preparedStatement.setString(cnt, field);
				cnt++;
				if (verbose) System.out.println(""+entry.getKey().toString()+":"+field);
			}

			if(entitySet.equalsIgnoreCase("users") && insertImage){
				InputStream is = null;
				byte[] profileImage = null;
				int image_size = -1;
				
				if(USE_RANDOM_BYTES_FOR_IMAGE)
				{
					// Take the image byte array as passed in to the insertEntity call
					profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
					is = new ByteArrayInputStream(profileImage);
					image_size = profileImage.length;
				}
				else
				{
					// Use an actual image. For debugging purposes.
					String filename = "C:/oracle/rays_offline_user0.png";
					image_size = (int)new File(filename).length();
					profileImage = new byte[image_size];
					is = new FileInputStream(filename);
					is.read(profileImage, 0, image_size);
					is.close();
					
					// Reset it back to an open stream in case of storing the image as BLOB
					is = new FileInputStream(filename);
				}
				
				if ( FSimagePath.equals("") )
					preparedStatement.setBinaryStream(numFields, is, image_size);
				else
					StoreImageInFS(entityPK, profileImage, true, FSimagePath);
				
				byte[] thumbImage = ((ObjectByteIterator)values.get("tpic")).toArray();
				is = new ByteArrayInputStream(thumbImage);
				
				if (FSimagePath.equals(""))
					preparedStatement.setBinaryStream(numFields+1, is, thumbImage.length);
				else
					StoreImageInFS(entityPK, thumbImage, false, FSimagePath);
			}
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			System.out.println("Error in processing insert to table: " + entitySet + e);
			return -2;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				return -1;
			}
		}
		return 0;
	}
	
	
	//Load phase query not cached
	public static int insertEntityBoosted(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage, 
			Connection conn, String FSimagePath) {
		if (entitySet == null) {
			return -1;
		}
		if (entityPK == null) {
			return -1;
		}
		
		PreparedStatement preparedStatement = null;
		ResultSet rs =null;

		try {
			String query;
			int numFields = values.size();
			
			// Remove the pic field and put it in the filesystem
			if(entitySet.equalsIgnoreCase("users") && insertImage && !FSimagePath.equals(""))
				numFields = numFields-1;
			
			query = "INSERT INTO "+entitySet+" VALUES (";
			//adding the three new attributes
			if(entitySet.equalsIgnoreCase("users"))
				numFields +=3;
			for(int j=0; j<=numFields; j++){
				if(j==(numFields)){
					query+="?)";
					break;
				}else
					query+="?,";
			}

			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setString(1, entityPK);
			int cnt=2;
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				//blobs need to be inserted differently
				if(entry.getKey().equalsIgnoreCase("pic") || entry.getKey().equalsIgnoreCase("tpic") )
					continue;
				
				String field = entry.getValue().toString();
				preparedStatement.setString(cnt, field);
				cnt++;
				if (verbose) System.out.println(""+entry.getKey().toString()+":"+field);
			}

			if(entitySet.equalsIgnoreCase("users") && insertImage){
				byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
				InputStream is = new ByteArrayInputStream(profileImage);
				if ( FSimagePath.equals("") ){
					preparedStatement.setBinaryStream(cnt, is, profileImage.length);
					cnt++;
				}
				else
					StoreImageInFS(entityPK, profileImage, true, FSimagePath);
				
				
				byte[] thumbImage = ((ObjectByteIterator)values.get("tpic")).toArray();
				is = new ByteArrayInputStream(thumbImage);
				
				
				preparedStatement.setBinaryStream(cnt, is, thumbImage.length);
				cnt++;
				
			}
			if(entitySet.equalsIgnoreCase("users")){
				//pendCount, confCount,resCount
				preparedStatement.setInt(cnt, 0);
				cnt++;
				preparedStatement.setInt(cnt, 0);
				cnt++;
				preparedStatement.setInt(cnt, 0);
				cnt++;
			}else{
				//increament the resource count for the walluserid
				String updateQ = "Update users set rescount=rescount+1 where userid="+values.get("walluserid");
				Statement st = conn.createStatement();
				st.executeUpdate(updateQ);
				st.close();
			}
			rs = preparedStatement.executeQuery();
		} catch (SQLException e) {
			System.out.println("Error in processing insert to table: " + entitySet + e);
			return -2;
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				return -1;
			}
		}
		return 0;
	}
	
	
	
	public static void createSchema(Properties props, Connection conn){

		Statement stmt = null;

		try {
			stmt = conn.createStatement();

			//dropSequence(stmt, "MIDINC");
			dropSequence(stmt, "RIDINC");
			dropSequence(stmt, "USERIDINC");
			dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			//stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID NUMBER, INVITEEID NUMBER,"
					+ "STATUS NUMBER DEFAULT 1" + ") NOLOGGING");

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID NUMBER," + "CREATORID NUMBER, RID NUMBER,"
					+ "MODIFIERID NUMBER, TIMESTAMP VARCHAR2(200),"
					+ "TYPE VARCHAR2(200), CONTENT VARCHAR2(200)"
					+ ") NOLOGGING");

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID NUMBER,CREATORID NUMBER,"
					+ "WALLUSERID NUMBER, TYPE VARCHAR2(200),"
					+ "BODY VARCHAR2(200), DOC VARCHAR2(200)"
					+ ") NOLOGGING");

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) ) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), PIC BLOB, TPIC BLOB"
						+ ") NOLOGGING");
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200)"
						+ ") NOLOGGING");

			}

			stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID,RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
		

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
					+ "for each row "
					+ "WHEN (new.rid is null) begin "
					+ "select ridInc.nextval into :new.rid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
					+ "for each row "
					+ "WHEN (new.userid is null) begin "
					+ "select useridInc.nextval into :new.userid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
//			//drop stored procedure
//			dropStoredProcedure(stmt, "INVITEFRIEND");
//			stmt.execute("CREATE OR REPLACE PROCEDURE VIEWPROFILE " +
//							"("+ 
//						         " PROFILEOWNERID   IN FRIENDSHIP.INVITERID%TYPE    , "+   
//						         " FRIENDCOUNT OUT INTEGER, " +
//						         " PENDINGCOUNT OUT INTEGER "+
//						     ")" +
//						"AS "+ 
//						"BEGIN "+ 
//						      "INSERT INTO FRIENDSHIP "+
//						             "("+ 
//						               "INVITERID     , "+
//						               "INVITEEID     , "+
//						               "STATUS"+
//						              ")"+ 
//						     " VALUES "+
//						             "("+ 
//						               "P_INVITERID     , "+
//						               "P_INVITEEID     , "+
//						               "1"+
//						             ");"+
//						      "RET_VAL := TRANS_ID_PKG.ret_val; " + 
//						      "IF (RET_VAL = 0) THEN " +
//						      "		  ROLLBACK;"+
//						      "ELSE "+
//						      		  "UPDATE USERS SET "+
//								  			"PENDCOUNT=PENDCOUNT+1 WHERE USERID = P_INVITEEID;"+
//								  	  "COMMIT;"+
//							  "END IF; "+
//						"END INVITEFRIEND;"
//					);
			
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}
	}

	public static void buildIndexes(Properties props, Connection conn){
		Statement stmt  = null;
		try {
			stmt = conn.createStatement();
			long startIdx = System.currentTimeMillis();

			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			stmt.executeUpdate("CREATE INDEX RESOURCE_CREATORID ON RESOURCES (CREATORID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP (INVITEEID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP (INVITERID)"
					+ "COMPUTE STATISTICS NOLOGGING");
			
			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_STATUS ON FRIENDSHIP (STATUS)"
					+ "COMPUTE STATISTICS NOLOGGING");

			
			stmt.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)"
					+ "COMPUTE STATISTICS NOLOGGING");
			
			stmt.executeUpdate("analyze table users compute statistics");
			stmt.executeUpdate("analyze table resources compute statistics");
			stmt.executeUpdate("analyze table friendship compute statistics");
			stmt.executeUpdate("analyze table manipulation compute statistics");
			long endIdx = System.currentTimeMillis();
			System.out
			.println("Time to build database index structures(ms):"
					+ (endIdx - startIdx));
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
	}

	private static void dropSequence(Statement st, String seqName) {
		try {
			st.executeUpdate("drop sequence " + seqName);
		} catch (SQLException e) {
		}
	}

	private static void dropIndex(Statement st, String idxName) {
		try {
			st.executeUpdate("drop index " + idxName);
		} catch (SQLException e) {
		}
	}

	private static void dropTable(Statement st, String tableName) {
		try {
			st.executeUpdate("drop table " + tableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private static void dropStoredProcedure(Statement st, String procName) {
		try {
			st.executeUpdate("drop procedure " + procName);
		} catch (SQLException e) {
		}
	}
	
	
	public static void createSchemaSqlServer(Properties props, Connection conn){

		Statement stmt = null;

		try {
			stmt = conn.createStatement();

			//dropSequence(stmt, "MIDINC");
			//dropSequence(stmt, "RIDINC");
			//dropSequence(stmt, "USERIDINC");
			//dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			//stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			//stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			//stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			//stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID INT NOT NULL, INVITEEID INT NOT NULL,"
					+ "STATUS INT DEFAULT 1" + ")");

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID INT NOT NULL," + "CREATORID INT NOT NULL, RID INT NOT NULL,"
					+ "MODIFIERID INT, TIMESTAMP VARCHAR(200),"
					+ "TYPE VARCHAR(200), CONTENT VARCHAR(200)"
					+ ")");

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID INT PRIMARY KEY, CREATORID INT NOT NULL,"
					+ "WALLUSERID INT, TYPE VARCHAR(200),"
					+ "BODY VARCHAR(200), DOC VARCHAR(200)"
					+ ")");

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) ) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INT PRIMARY KEY, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200), PIC image, TPIC image"
						+ ")");
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INT PRIMARY KEY, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200)"
						+ ")");

			}

			//stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID,RID)");
			//stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID)");
			//stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ");
			//stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			//stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE NO ACTION ");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE NO ACTION ");
			// TODO: make these two constraints work with SQL Server
//			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
//					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ");
//			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
//					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ");
		
//
//			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
//					+ "for each row "
//					+ "WHEN (new.rid is null) begin "
//					+ "select ridInc.nextval into :new.rid from dual;"
//					+ "end;");
//			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");
//
//			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
//					+ "for each row "
//					+ "WHEN (new.userid is null) begin "
//					+ "select useridInc.nextval into :new.userid from dual;"
//					+ "end;");
//			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}
	}

	public static void buildIndexesSqlServer(Properties props, Connection conn){
		Statement stmt  = null;
		try {
			stmt = conn.createStatement();
			long startIdx = System.currentTimeMillis();

			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			stmt.executeUpdate("CREATE INDEX RESOURCE_CREATORID ON RESOURCES (CREATORID)"
					+ " ");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP (INVITEEID)"
					+ " ");

			stmt.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)"
					+ " ");

			stmt.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)"
					+ " ");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP (INVITERID)"
					+ " ");

			
			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_STATUS ON FRIENDSHIP (STATUS)"
					+ " ");

			
			stmt.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)"
					+ " ");
			
			stmt.executeUpdate("CREATE STATISTICS users_stats ON users (userid)");
			stmt.executeUpdate("CREATE STATISTICS resources_stats ON resources (rid, CREATORID)");
			stmt.executeUpdate("CREATE STATISTICS friendship_stats ON friendship (inviterid, inviteeid, status)");
			stmt.executeUpdate("CREATE STATISTICS manipulation_stats ON manipulation (MID, CREATORID, RID, MODIFIERID)");
			long endIdx = System.currentTimeMillis();
			System.out
			.println("Time to build database index structures(ms):"
					+ (endIdx - startIdx));
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
	}
	
	public static void createSchemaSXQLeasesTrigger(Properties props, Connection conn) {
		Statement stmt = null;
		String logging = "NOLOGGING";
		//String logging = "";

		try {
			
			stmt = conn.createStatement();

			//dropSequence(stmt, "MIDINC");
			dropSequence(stmt, "RIDINC");
			dropSequence(stmt, "USERIDINC");
			dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			//stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID NUMBER, INVITEEID NUMBER,"
					+ "STATUS NUMBER DEFAULT 1" + ") " + logging);

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID NUMBER," + "CREATORID NUMBER, RID NUMBER,"
					+ "MODIFIERID NUMBER, TIMESTAMP VARCHAR2(200),"
					+ "TYPE VARCHAR2(200), CONTENT VARCHAR2(200)"
					+ ") " + logging);

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID NUMBER,CREATORID NUMBER,"
					+ "WALLUSERID NUMBER, TYPE VARCHAR2(200),"
					+ "BODY VARCHAR2(200), DOC VARCHAR2(200)"
					+ ") " + logging);

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), PIC BLOB, TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			}
			else if(Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					!props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200),"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);

			}

			stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID, RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
					+ "for each row "
					+ "WHEN (new.rid is null) begin "
					+ "select ridInc.nextval into :new.rid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
					+ "for each row "
					+ "WHEN (new.userid is null) begin "
					+ "select useridInc.nextval into :new.userid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
			// create TRANS_ID_PKG for storing trans_id to pass to trigger
			stmt.executeUpdate(
					"CREATE OR REPLACE PACKAGE TRANS_ID_PKG IS " +
					"	trans_id VARCHAR2(100); " +
					"	ret_val INTEGER; "+
					"END TRANS_ID_PKG;"
			);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "INVITEFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INVITEFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  , "+
						         " TRANS_ID 	IN VARCHAR2, "+
						         " RET_VAL OUT INTEGER "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "TRANS_ID_PKG.trans_id := TRANS_ID;" +
						
						      "INSERT INTO FRIENDSHIP "+
						             "("+ 
						               "INVITERID     , "+
						               "INVITEEID     , "+
						               "STATUS"+
						              ")"+ 
						     " VALUES "+
						             "("+ 
						               "P_INVITERID     , "+
						               "P_INVITEEID     , "+
						               "1"+
						             ");"+
						      "RET_VAL := TRANS_ID_PKG.ret_val; " + 
						      "IF (TRANS_ID_PKG.ret_val = 0) THEN " +
						      "		  ROLLBACK;"+
						      "ELSE "+
						      		  "UPDATE USERS SET "+
								  			"PENDCOUNT=PENDCOUNT+1 WHERE USERID = P_INVITEEID;"+
								  	  "COMMIT;"+
							  "END IF; "+
						"END INVITEFRIEND;"
					);
		
			//drop stored procedure
			dropStoredProcedure(stmt, "ACCEPTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE ACCEPTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  , "+
						         " TRANS_ID 	IN VARCHAR2, "+
						         " RET_VAL OUT INTEGER "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "TRANS_ID_PKG.trans_id := TRANS_ID;" +
						
							  "UPDATE FRIENDSHIP SET STATUS=2 "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID ;"+
							  
							  "RET_VAL := TRANS_ID_PKG.ret_val; "+
							  "IF (RET_VAL = 0) THEN "+
							  "		ROLLBACK; "+
							  "ELSE "+
								  	  "INSERT INTO friendship VALUES(P_INVITEEID,P_INVITERID,2);"+
									  "RET_VAL := TRANS_ID_PKG.ret_val; "+
									  "IF (RET_VAL = 0) THEN "+
									  "		ROLLBACK; "+
									  "ELSE "+
										  "UPDATE USERS SET "+
										  		"CONFCOUNT=CONFCOUNT+1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
										  "UPDATE USERS SET "+
											  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
									      "COMMIT; "+
									  "END IF; "+
						      "END IF; "+
						"END ACCEPTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "REJECTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE REJECTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  , "+
						         " TRANS_ID IN VARCHAR2, "+
						         " RET_VAL OUT INTEGER "+
						     ")" +
						"AS "+ 
						"BEGIN "+
							  "TRANS_ID_PKG.trans_id := TRANS_ID;" +
						
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID AND STATUS=1 ;"+
							  
							  "RET_VAL := TRANS_ID_PKG.ret_val; "+
							  "IF (RET_VAL = 0) THEN "+
							  "		ROLLBACK; "+
							  "ELSE "+
								  "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
							      "COMMIT;"+
							  "END IF; "+
						"END REJECTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "THAWFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE THAWFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  , "+
						         " TRANS_ID IN VARCHAR2, "+
						         " RET_VAL OUT INTEGER"+
						     ")" +
						"AS "+ 
						"BEGIN "+
							  "TRANS_ID_PKG.trans_id := TRANS_ID;" +
						
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE (INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID) AND STATUS=2;"+
			
							  "RET_VAL := TRANS_ID_PKG.ret_val; "+
							  "IF (RET_VAL = 0) THEN "+
							  "		ROLLBACK; "+
							  "ELSE "+
								  "DELETE FROM FRIENDSHIP " +
								  		"WHERE (INVITERID = P_INVITEEID AND INVITEEID = P_INVITERID) AND STATUS=2;"+
								  
								  "RET_VAL := TRANS_ID_PKG.ret_val; "+
								  
								  "IF (RET_VAL = 0) THEN "+
								  "		ROLLBACK; "+
								  "ELSE "+
									  "UPDATE USERS SET "+
									  		"CONFCOUNT=CONFCOUNT-1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+ 
								      "COMMIT; "+
								  "END IF; "+
						      "END IF; "+
						"END THAWFRIEND;"
					);
			
			dropStoredProcedure(stmt, "INSERTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INSERTFRIEND " +
					"("+ 
					" P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
					" p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
					")" +
					"AS "+ 
					"BEGIN "+ 
					"	INSERT INTO FRIENDSHIP (INVITERID, INVITEEID, STATUS)"+ 
					" 		VALUES (P_INVITERID, P_INVITEEID, 2);"+ 
					"	INSERT INTO FRIENDSHIP (INVITERID, INVITEEID, STATUS)"+ 
					" 		VALUES (P_INVITEEID, P_INVITERID, 2);"+ 
					"	UPDATE USERS SET  CONFCOUNT=CONFCOUNT+1 " +
					"		WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
					"	COMMIT;"+
					"END INSERTFRIEND;"
					);
			
//			//Deletes keys impacted by InviteFriend action when insertimage=true
//			stmt.execute("CREATE OR REPLACE TRIGGER InviteFriend "+
//					"BEFORE INSERT ON friendship "+
//					"FOR EACH ROW "+
//					"DECLARE "+
//					"   k3 CLOB := TO_CLOB('lsFrdsNoImage'); "+						
//					"   k4 CLOB := TO_CLOB('profileNoImage'); "+
//					"	ret_val BINARY_INTEGER; "+
//					"   trans_id VARCHAR2(100); " +
//					"   DELETEKEY CLOB; " +
//					"BEGIN "+
//					"	trans_id := TRANS_ID_PKG.trans_id; "+
//					"   k3 := CONCAT(k3, :NEW.inviteeid); "+
//					"   k4 := CONCAT(k4, :NEW.inviteeid); "+						
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k4));  "+
//					"   ret_val := QuarantineAndRegister('RAYS', trans_id, DELETEKEY, 0); "+
//					"end; ");
//			
//			//Deletes those keys impacted by Accept Friend Req
//			stmt.execute("create or replace trigger AcceptFriendReq "+
//					"before UPDATE on friendship "+
//					"for each row "+
//					"declare "+
//					"   k1 CLOB := TO_CLOB('lsFrdsNoImage'); "+
//					"   k2 CLOB := TO_CLOB('lsFrdsNoImage'); "+
//					"   k3 CLOB := TO_CLOB('profileNoImage'); "+
//					"   k4 CLOB := TO_CLOB('profileNoImage'); "+
//					"   k7 CLOB := TO_CLOB('viewPendReqNoImage'); "+
//					"   trans_id VARCHAR2(100); "+
//					"   DELETEKEY CLOB; "+
//					"	ret_val BINARY_INTEGER; "+
//					"begin "+
//					"   trans_id := TRANS_ID_PKG.trans_id; "+
//					"   k1 := CONCAT(k1, :NEW.inviterid); "+
//					"   k2 := CONCAT(k2, :NEW.inviteeid); "+
//					"   k3 := CONCAT(k3, :NEW.inviterid); "+
//					"   k4 := CONCAT(k4, :NEW.inviteeid); "+
//					"   k7 := CONCAT(k7, :NEW.inviteeid); "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k1));  "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k2));  "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k4));  "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k7));  "+
//					"   ret_val := QuarantineAndRegister('RAYS', trans_id, DELETEKEY, 0); "+
//					"end;");
//			
//			//Deletes those keys impacted by two actions:  1)Reject Friend Request and  2) Thaw Friendship
//			stmt.execute("create or replace trigger RejFrdReqThawFrd "+
//					"before DELETE on friendship "+
//					"for each row "+
//					"declare "+
//					"   k2 CLOB := TO_CLOB('profileNoImage'); "+
//					"   k3 CLOB := TO_CLOB('viewPendReqNoImage'); "+
//					"   k12 CLOB := TO_CLOB('lsFrdsNoImage'); "+
//					"   k13 CLOB := TO_CLOB('lsFrdsNoImage'); "+
//					"   k14 CLOB := TO_CLOB('profileNoImage'); "+
//					"   k15 CLOB := TO_CLOB('profileNoImage'); "+
//					"   trans_id VARCHAR2(100); "+
//					"   DELETEKEY CLOB; "+
//					"	ret_val BINARY_INTEGER; "+
//					"begin "+
//					"   trans_id = TRANS_ID_PKG.trans_id; "+
//					"IF(:OLD.status = 1) THEN "+
//					"   k2 := CONCAT(k2, :OLD.inviteeid); "+
//					"   k3 := CONCAT(k3, :OLD.inviteeid); "+	
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k2));  "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+
//					
//					"ELSE "+						
//					"   k12 := CONCAT(k12, :OLD.inviterid); "+
//					"   k13 := CONCAT(k13, :OLD.inviteeid); "+						
//					"   k14 := CONCAT(k14, :OLD.inviterid); "+
//					"   k15 := CONCAT(k15, :OLD.inviteeid); "+					
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k12));  "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k13));  "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k14));  "+
//					"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k15));  "+
//					"END IF; "+
//					
//					"   ret_val := QuarantineAndRegister('RAYS', trans_id, DELETEKEY, 0); "+
//					"end;");
//			
//			//Deletes keys impacted by the Post Command Action of BG
//			stmt.execute("create or replace trigger PostComments "+
//					"before INSERT on manipulation "+
//					"for each row "+
//					"declare "+
//					"   k1 CLOB := TO_CLOB('ResCmts'); "+
//					"   trans_id VARCHAR2(100); "+
//					"	ret_val BINARY_INTEGER; "+
//					"begin "+
//					"   trans_id := TRANS_ID_PKG.trans_id; "+
//					"   k1 := CONCAT(k1, :NEW.rid); "+
//					"   ret_val := QuarantineAndRegister('RAYS', trans_id, k1, 0); "+
//					"end;");			
			
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}
	}
	
	public static void createSchemaRefreshTechnique(Properties props, Connection conn) {
		Statement stmt = null;
		String logging = "NOLOGGING";
		//String logging = "";

		try {
			
			stmt = conn.createStatement();

			//dropSequence(stmt, "MIDINC");
			dropSequence(stmt, "RIDINC");
			dropSequence(stmt, "USERIDINC");
			dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			//stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID NUMBER, INVITEEID NUMBER,"
					+ "STATUS NUMBER DEFAULT 1" + ") " + logging);

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID NUMBER," + "CREATORID NUMBER, RID NUMBER,"
					+ "MODIFIERID NUMBER, TIMESTAMP VARCHAR2(200),"
					+ "TYPE VARCHAR2(200), CONTENT VARCHAR2(200)"
					+ ") " + logging);

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID NUMBER,CREATORID NUMBER,"
					+ "WALLUSERID NUMBER, TYPE VARCHAR2(200),"
					+ "BODY VARCHAR2(200), DOC VARCHAR2(200)"
					+ ") " + logging);

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), PIC BLOB, TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			}
			else if(Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					!props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200),"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);

			}

			stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID, RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
					+ "for each row "
					+ "WHEN (new.rid is null) begin "
					+ "select ridInc.nextval into :new.rid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
					+ "for each row "
					+ "WHEN (new.userid is null) begin "
					+ "select useridInc.nextval into :new.userid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
			//drop stored procedure
			dropStoredProcedure(stmt, "INVITEFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INVITEFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
						      "INSERT INTO FRIENDSHIP "+
						             "("+ 
						               "INVITERID     , "+
						               "INVITEEID     , "+
						               "STATUS"+
						              ")"+ 
						     " VALUES "+
						             "("+ 
						               "P_INVITERID     , "+
						               "P_INVITEEID     ,"+
						               "1"+
						             ");"+ 
						      "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT+1 WHERE USERID = P_INVITEEID;"+
						"END INVITEFRIEND;"
					);
		
			//drop stored procedure
			dropStoredProcedure(stmt, "ACCEPTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE ACCEPTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "UPDATE FRIENDSHIP SET STATUS=2 "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID ;"+
						  	  "INSERT INTO friendship VALUES(P_INVITEEID,P_INVITERID,2);"+
							  "UPDATE USERS SET "+
							  		"CONFCOUNT=CONFCOUNT+1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
							  "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						"END ACCEPTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "REJECTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE REJECTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID AND STATUS=1 ;"+
							  "UPDATE USERS SET "+
							  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						"END REJECTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "THAWFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE THAWFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE (INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID) OR (INVITERID = P_INVITEEID AND INVITEEID = P_INVITERID) AND STATUS=2;"+
							  "UPDATE USERS SET "+
							  		"CONFCOUNT=CONFCOUNT-1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+ 
						"END THAWFRIEND;"
					);
			
			dropStoredProcedure(stmt, "INSERTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INSERTFRIEND " +
					"("+ 
					" P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
					" p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
					")" +
					"AS "+ 
					"BEGIN "+ 
					"	INSERT INTO FRIENDSHIP (INVITERID, INVITEEID, STATUS)"+ 
					" 		VALUES (P_INVITERID, P_INVITEEID, 2);"+ 
					"	INSERT INTO FRIENDSHIP (INVITERID, INVITEEID, STATUS)"+ 
					" 		VALUES (P_INVITEEID, P_INVITERID, 2);"+ 
					"	UPDATE USERS SET  CONFCOUNT=CONFCOUNT+1 " +
					"		WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
					"END INSERTFRIEND;"
					);
			
			
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}		
	}
	
	public static void createSchemaBoosted2R1T(Properties props, Connection conn){

		Statement stmt = null;
		String logging = "NOLOGGING";
		//String logging = "";

		try {
			
			stmt = conn.createStatement();

			//dropSequence(stmt, "MIDINC");
			dropSequence(stmt, "RIDINC");
			dropSequence(stmt, "USERIDINC");
			dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			//stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID NUMBER, INVITEEID NUMBER,"
					+ "STATUS NUMBER DEFAULT 1" + ") " + logging);

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID NUMBER," + "CREATORID NUMBER, RID NUMBER,"
					+ "MODIFIERID NUMBER, TIMESTAMP VARCHAR2(200),"
					+ "TYPE VARCHAR2(200), CONTENT VARCHAR2(200)"
					+ ") " + logging);

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID NUMBER,CREATORID NUMBER,"
					+ "WALLUSERID NUMBER, TYPE VARCHAR2(200),"
					+ "BODY VARCHAR2(200), DOC VARCHAR2(200)"
					+ ") " + logging);

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), PIC BLOB, TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			}
			else if(Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					!props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200),"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);

			}

			stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID, RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
					+ "for each row "
					+ "WHEN (new.rid is null) begin "
					+ "select ridInc.nextval into :new.rid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
					+ "for each row "
					+ "WHEN (new.userid is null) begin "
					+ "select useridInc.nextval into :new.userid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
			//drop stored procedure
			dropStoredProcedure(stmt, "INVITEFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INVITEFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
						      "INSERT INTO FRIENDSHIP "+
						             "("+ 
						               "INVITERID     , "+
						               "INVITEEID     , "+
						               "STATUS"+
						              ")"+ 
						     " VALUES "+
						             "("+ 
						               "P_INVITERID     , "+
						               "P_INVITEEID     ,"+
						               "1"+
						             ");"+ 
						      "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT+1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END INVITEFRIEND;"
					);
		
			//drop stored procedure
			dropStoredProcedure(stmt, "ACCEPTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE ACCEPTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "UPDATE FRIENDSHIP SET STATUS=2 "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID ;"+
						  	  "INSERT INTO friendship VALUES(P_INVITEEID,P_INVITERID,2);"+
							  "UPDATE USERS SET "+
							  		"CONFCOUNT=CONFCOUNT+1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
							  "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END ACCEPTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "REJECTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE REJECTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID AND STATUS=1 ;"+
							  "UPDATE USERS SET "+
							  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END REJECTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "THAWFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE THAWFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE (INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID) OR (INVITERID = P_INVITEEID AND INVITEEID = P_INVITERID) AND STATUS=2;"+
							  "UPDATE USERS SET "+
							  		"CONFCOUNT=CONFCOUNT-1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+ 
						      "COMMIT;"+
						"END THAWFRIEND;"
					);
			
			dropStoredProcedure(stmt, "INSERTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INSERTFRIEND " +
					"("+ 
					" P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
					" p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
					")" +
					"AS "+ 
					"BEGIN "+ 
					"	INSERT INTO FRIENDSHIP (INVITERID, INVITEEID, STATUS)"+ 
					" 		VALUES (P_INVITERID, P_INVITEEID, 2);"+ 
					"	INSERT INTO FRIENDSHIP (INVITERID, INVITEEID, STATUS)"+ 
					" 		VALUES (P_INVITEEID, P_INVITERID, 2);"+ 
					"	UPDATE USERS SET  CONFCOUNT=CONFCOUNT+1 " +
					"		WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
					"	COMMIT;"+
					"END INSERTFRIEND;"
					);
			
			
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}
	}		
	
	public static void createSchemaBoosted2R1TRefreshTechnique(Properties props, Connection conn){

		Statement stmt = null;
		String logging = "NOLOGGING";
		//String logging = "";

		try {
			
			stmt = conn.createStatement();

			//dropSequence(stmt, "MIDINC");
			dropSequence(stmt, "RIDINC");
			dropSequence(stmt, "USERIDINC");
			dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			//stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID NUMBER, INVITEEID NUMBER,"
					+ "STATUS NUMBER DEFAULT 1" + ") " + logging);

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID NUMBER," + "CREATORID NUMBER, RID NUMBER,"
					+ "MODIFIERID NUMBER, TIMESTAMP VARCHAR2(200),"
					+ "TYPE VARCHAR2(200), CONTENT VARCHAR2(200)"
					+ ") " + logging);

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID NUMBER,CREATORID NUMBER,"
					+ "WALLUSERID NUMBER, TYPE VARCHAR2(200),"
					+ "BODY VARCHAR2(200), DOC VARCHAR2(200)"
					+ ") " + logging);

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), PIC BLOB, TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			}
			else if(Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					!props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200),"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);

			}

			stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID, RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
					+ "for each row "
					+ "WHEN (new.rid is null) begin "
					+ "select ridInc.nextval into :new.rid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
					+ "for each row "
					+ "WHEN (new.userid is null) begin "
					+ "select useridInc.nextval into :new.userid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
			//drop stored procedure
			dropStoredProcedure(stmt, "INVITEFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INVITEFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
						      "INSERT INTO FRIENDSHIP "+
						             "("+ 
						               "INVITERID     , "+
						               "INVITEEID     , "+
						               "STATUS"+
						              ")"+ 
						     " VALUES "+
						             "("+ 
						               "P_INVITERID     , "+
						               "P_INVITEEID     ,"+
						               "1"+
						             ");"+ 
						      "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT+1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END INVITEFRIEND;"
					);
		
			//drop stored procedure
			dropStoredProcedure(stmt, "ACCEPTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE ACCEPTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "UPDATE FRIENDSHIP SET STATUS=2 "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID ;"+
						  	  "INSERT INTO friendship VALUES(P_INVITEEID,P_INVITERID,2);"+
							  "UPDATE USERS SET "+
							  		"CONFCOUNT=CONFCOUNT+1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
							  "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END ACCEPTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "REJECTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE REJECTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID AND STATUS=1 ;"+
							  "UPDATE USERS SET "+
							  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END REJECTFRIEND;"
					);
			
			//dropStoredProcedure
			dropStoredProcedure(stmt, "DELETECOMMENT");
			stmt.execute("CREATE OR REPLACE PROCEDURE DELETECOMMENT" +
					      	"("+
					      		" MANID IN MANIPULATION.MID%TYPE, "+
					      		" RESOURCEID IN MANIPULATION.RID%TYPE, "+
					      		" STAT OUT NUMBER"+
					      	")" +
					      "AS "+
					      "BEGIN "+
					      		"DELETE FROM MANIPULATION "+
					      			"WHERE (MID = MANID AND RID = RESOURCEID);"+
					      		"STAT := SQL%ROWCOUNT;"+
					      		"COMMIT;"+
					      "END DELETECOMMENT;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "THAWFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE THAWFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  , "+
						         " stat OUT NUMBER"+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE (INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID) OR (INVITERID = P_INVITEEID AND INVITEEID = P_INVITERID) AND STATUS=2;"+
							  "stat := SQL%ROWCOUNT;" +
							  "UPDATE USERS SET "+
							  		"CONFCOUNT=CONFCOUNT-1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+ 
						      "COMMIT;"+
						"END THAWFRIEND;"
					);
			
			dropStoredProcedure(stmt, "INSERTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INSERTFRIEND " +
					"("+ 
					" P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
					" p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
					")" +
					"AS "+ 
					"BEGIN "+ 
					"	INSERT INTO FRIENDSHIP (INVITERID, INVITEEID, STATUS)"+ 
					" 		VALUES (P_INVITERID, P_INVITEEID, 2);"+ 
					"	INSERT INTO FRIENDSHIP (INVITERID, INVITEEID, STATUS)"+ 
					" 		VALUES (P_INVITEEID, P_INVITERID, 2);"+ 
					"	UPDATE USERS SET  CONFCOUNT=CONFCOUNT+1 " +
					"		WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
					"	COMMIT;"+
					"END INSERTFRIEND;"
					);
			
			
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}
	}
	
	public static void createSchemaBoosted(Properties props, Connection conn){

		Statement stmt = null;
		String logging = "NOLOGGING";
		//String logging = "";

		try {
			
			stmt = conn.createStatement();

			//dropSequence(stmt, "MIDINC");
			dropSequence(stmt, "RIDINC");
			dropSequence(stmt, "USERIDINC");
			dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			//stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID NUMBER, INVITEEID NUMBER,"
					+ "STATUS NUMBER DEFAULT 1" + ") " + logging);

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID NUMBER," + "CREATORID NUMBER, RID NUMBER,"
					+ "MODIFIERID NUMBER, TIMESTAMP VARCHAR2(200),"
					+ "TYPE VARCHAR2(200), CONTENT VARCHAR2(200)"
					+ ") " + logging);

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID NUMBER,CREATORID NUMBER,"
					+ "WALLUSERID NUMBER, TYPE VARCHAR2(200),"
					+ "BODY VARCHAR2(200), DOC VARCHAR2(200)"
					+ ") " + logging);

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), PIC BLOB, TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			}
			else if(Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && 
					!props.getProperty(RDBMSClientConstants.FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200),"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") " + logging);

			}

			stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID, RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
					+ "for each row "
					+ "WHEN (new.rid is null) begin "
					+ "select ridInc.nextval into :new.rid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
					+ "for each row "
					+ "WHEN (new.userid is null) begin "
					+ "select useridInc.nextval into :new.userid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
			//drop stored procedure
			dropStoredProcedure(stmt, "INVITEFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INVITEFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
						      "INSERT INTO FRIENDSHIP "+
						             "("+ 
						               "INVITERID     , "+
						               "INVITEEID     , "+
						               "STATUS"+
						              ")"+ 
						     " VALUES "+
						             "("+ 
						               "P_INVITERID     , "+
						               "P_INVITEEID     ,"+
						               "1"+
						             ");"+ 
						      "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT+1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END INVITEFRIEND;"
					);
		
			//drop stored procedure
			dropStoredProcedure(stmt, "ACCEPTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE ACCEPTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "UPDATE FRIENDSHIP SET STATUS=2 "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID ;"+
							  "UPDATE USERS SET "+
							  		"CONFCOUNT=CONFCOUNT+1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
							  "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END ACCEPTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "REJECTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE REJECTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID AND STATUS=1 ;"+
							  "UPDATE USERS SET "+
							  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END REJECTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "THAWFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE THAWFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM FRIENDSHIP "+
							  		"WHERE (INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID) OR (INVITERID = P_INVITEEID AND INVITEEID = P_INVITERID) AND STATUS=2;"+
							  "UPDATE USERS SET "+
							  		"CONFCOUNT=CONFCOUNT-1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+ 
						      "COMMIT;"+
						"END THAWFRIEND;"
					);
			
			dropStoredProcedure(stmt, "INSERTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INSERTFRIEND " +
							"("+ 
						         " P_INVITERID   IN FRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN FRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
						 "INSERT INTO FRIENDSHIP "+
			             "("+ 
			               "INVITERID     , "+
			               "INVITEEID     , "+
			               "STATUS"+
			              ")"+ 
			     " VALUES "+
			             "("+ 
			               "P_INVITERID     , "+
			               "P_INVITEEID     ,"+
			               "2"+
			             ");"+ 
			      "UPDATE USERS SET "+
					  		"CONFCOUNT=CONFCOUNT+1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
						      "COMMIT;"+
						"END INSERTFRIEND;"
					);
			
			
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}
	}
	
	public static HashMap<String, String> getInitialStats(Connection conn)
	{
		HashMap<String, String> stats = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		try {
			st = conn.createStatement();
			//get user count
			query = "SELECT count(*) from users";
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("usercount",rs.getString(1));
			}else
				stats.put("usercount","0"); //sth is wrong - schema is missing
			if(rs != null ) rs.close();
			//get user offset
			query = "SELECT min(userid) from users";
			rs = st.executeQuery(query);
			String offset = "0";
			if(rs.next()){
				offset = rs.getString(1);
			}
			//get resources per user
			query = "SELECT count(*) from resources where creatorid="+Integer.parseInt(offset);
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("resourcesperuser",rs.getString(1));
			}else{
				stats.put("resourcesperuser","0");
			}
			if(rs != null) rs.close();	
			//get number of friends per user
			query = "select count(*) from friendship where (inviterid="+Integer.parseInt(offset) +" OR inviteeid="+Integer.parseInt(offset) +") AND status=2" ;
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("avgfriendsperuser",rs.getString(1));
			}else
				stats.put("avgfriendsperuser","0");
			if(rs != null) rs.close();
			query = "select count(*) from friendship where (inviteeid="+Integer.parseInt(offset) +") AND status=1" ;
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("avgpendingperuser",rs.getString(1));
			}else
				stats.put("avgpendingperuser","0");
			

		}catch(SQLException sx){
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}

		}
		return stats;
	}
	
	public static HashMap<String, String> getInitialStats2R1T(Connection conn)
	{
		HashMap<String, String> stats = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		try {
			st = conn.createStatement();
			//get user count
			query = "SELECT count(*) from users";
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("usercount",rs.getString(1));
			}else
				stats.put("usercount","0"); //sth is wrong - schema is missing
			if(rs != null ) rs.close();
			//get user offset
			query = "SELECT min(userid) from users";
			rs = st.executeQuery(query);
			String offset = "0";
			if(rs.next()){
				offset = rs.getString(1);
			}
			//get resources per user
			
			System.out.println(offset);
			query = "SELECT count(*) from resources where creatorid="+Integer.parseInt(offset);
			System.out.println(query);
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("resourcesperuser",rs.getString(1));
			}else{
				stats.put("resourcesperuser","0");
			}
			if(rs != null) rs.close();	
			//get number of friends per user
			query = "select count(*) from friendship where inviterid="+Integer.parseInt(offset) +" AND status=2" ;
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("avgfriendsperuser",rs.getString(1));
			}else
				stats.put("avgfriendsperuser","0");
			if(rs != null) rs.close();
			query = "select count(*) from friendship where (inviteeid="+Integer.parseInt(offset) +") AND status=1" ;
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("avgpendingperuser",rs.getString(1));
			}else
				stats.put("avgpendingperuser","0");
			

		}catch(SQLException sx){
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}

		}
		return stats;
	}
			
	public static HashMap<String, String> getInitialStatsBoosted(Connection conn) {
		HashMap<String, String> stats = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		try {
			st = conn.createStatement();
			//get user count
			query = "SELECT count(*) from users";
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("usercount",rs.getString(1));
			}else
				stats.put("usercount","0"); //sth is wrong - schema is missing
			if(rs != null ) rs.close();
			
			//get resources per user
			query = "SELECT avg(rescount) from users";
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("resourcesperuser",rs.getString(1));
			}else{
				stats.put("resourcesperuser","0");
			}
			if(rs != null) rs.close();	
			//get number of friends per user
			query = "select avg(confcount) from users" ;
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("avgfriendsperuser",rs.getString(1));
			}else
				stats.put("avgfriendsperuser","0");
			if(rs != null) rs.close();
			query = "select avg(pendcount) from users" ;
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("avgpendingperuser",rs.getString(1));
			}else
				stats.put("avgpendingperuser","0");
			

		}catch(SQLException sx){
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}

		}
		return stats;
	}
}
