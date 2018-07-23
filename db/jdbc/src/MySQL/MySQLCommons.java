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

public class MySQLCommons {

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
		try{
			imgctrl.acquire();
			FileOutputStream fos = new FileOutputStream(ImageFileName);
			fos.write(image);
			fos.close();
		}catch(Exception ex){
			System.out.println("Error in writing the file"+ImageFileName);
			ex.printStackTrace(System.out);
			imgctrl.release();
		}finally{
			imgctrl.release();
		}
		return result;
	}
	
	public static byte[] GetImageFromFS(String userid, boolean profileimg, String FSimagePath){
        int filelength = 0;
        String ext = "thumbnail";
        byte[] imgpayload = null;
       
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
				e.printStackTrace();
				imgctrl.release();
			}finally{
				imgctrl.release();
			}
       }
       return imgpayload;
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
	
	
	
	public static void dropSequence(Statement st, String seqName) {
		try {
			st.executeUpdate("drop sequence " + seqName);
		} catch (SQLException e) {
		}
	}

	public static void dropIndex(Statement st, String idxName, String tableName) {
		try {
			st.executeUpdate("ALTER TABLE " + tableName + " DROP INDEX " + idxName  );
		} catch (SQLException e) {
		}
	}

	public static void dropTable(Statement st, String tableName) {
		try {
			st.executeUpdate("drop table " + tableName);
		} catch (SQLException e) {
		}
	}
	
	public static void dropStoredProcedure(Statement st, String procName) {
		try {
			st.executeUpdate("drop procedure " + procName);
		} catch (SQLException e) {
		}
	}
	
	public static void dropConstraint(Statement st, String constName, String tableName) {
		try {
			st.executeUpdate("ALTER TABLE " + tableName +" DROP FOREIGN KEY "+constName);
		} catch (SQLException e) {
		}
	}
	
	
	
}