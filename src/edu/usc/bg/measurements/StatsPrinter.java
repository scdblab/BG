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


package edu.usc.bg.measurements;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

/**
 * Write human readable text. Tries to emulate the previous print report method.
 */
public class StatsPrinter
{

	private BufferedWriter bw;

	public StatsPrinter(OutputStream out)
	{
		this.bw = new BufferedWriter(new OutputStreamWriter(out));
	}

	public void write(String metric, String measurement, int i) throws IOException
	{
		bw.write("[" + metric + "], " + measurement + ", " + i);
		bw.newLine();
		bw.flush();
	}

	public void write(String metric, String measurement, double d) throws IOException
	{
		bw.write("[" + metric + "], " + measurement + ", " + d);
		bw.newLine();
		bw.flush();
	}

	public void write(String line) throws IOException{
		bw.write(line);
		bw.newLine();
		bw.flush();
	}


	public void close() throws IOException
	{
		this.bw.close();
	}

}
