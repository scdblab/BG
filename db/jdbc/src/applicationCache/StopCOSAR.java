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
package applicationCache;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.Date;

public class StopCOSAR extends Thread {
	private String cmd = "";
	private String filename = "";

	public StopCOSAR (String inputcmd)
	{
		cmd = inputcmd;
	}

	public StopCOSAR (String inputcmd, String outputfile)
	{
		cmd = inputcmd;
		filename = outputfile;
	}


	public void run()
	{
		String line;
		try {
			Process p = Runtime.getRuntime().exec(cmd);
			BufferedReader input = new BufferedReader (new InputStreamReader(p.getInputStream()));

			FileWriter fout = null;
			if( !filename.equals("") )
			{
				fout = new FileWriter(filename, true);
				fout.write("\r\n\r\n" + new Date().toString() + "\r\n");
			}


			while ( (line = input.readLine()) != null) {
				//System.out.println(line);
				if( fout != null )
				{
					fout.write(line + "\r\n");
				}
			}
			input.close();

			if( fout != null )
			{
				fout.flush();
				fout.close();
			}

			p.waitFor(); //Causes this thread to wait until COSAR terminates
		}
		catch (Exception err) {
			err.printStackTrace(System.out);
		}
	}
}


