package edu.usc.bg.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 String TASKLIST = "tasklist";
		 Process p=null;
		try {
			p = Runtime.getRuntime().exec("jps");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 BufferedReader reader = new BufferedReader(new InputStreamReader(
		   p.getInputStream()));
		 String line;
		 try {
			while ((line = reader.readLine()) != null) {
			 
			  if (line.contains("BGMainClass"))
			  { 
				  String pid=line.substring(0,line.indexOf(' '));
				  Process p2=Runtime.getRuntime().exec("taskkill /F /pid " +pid);
				  
			  }

}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

}
}
