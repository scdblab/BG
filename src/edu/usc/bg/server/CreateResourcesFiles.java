package edu.usc.bg.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Vector;

public class CreateResourcesFiles {
	public static Vector<StringBuilder> createFiles(String path, String fileNamePattern){
		Vector<StringBuilder> sv=new Vector<StringBuilder>();
//		Vector<File> filesVector=new Vector<File>();
		File srcFolder = new File(path);
		String files[] = srcFolder.list();
		for (String file : files) {
			File srcFile = new File(srcFolder, file);
			if (!srcFile.isDirectory()&& srcFile.getName().contains(fileNamePattern)){
//				filesVector.add(srcFile);
			StringBuilder s=new StringBuilder();
			readFile(srcFile,s);
			sv.add(s);
			}
		}
	
		return sv;
		
			
			
		
	}

	private static void readFiles(Vector<File> filesVector) {
//		
//		// TODO Auto-generated method stub
//
//		Vector<BufferedReader> brs=new Vector<BufferedReader>();
//		FileInputStream fis=null;
//	
//		for (int i=0;i<filesVector.size();i++)
//		{
//		//Construct BufferedReader from InputStreamReader
//		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filesVector.get(i))));
//		brs.add(br);
//		}
//		String[]lines=new String[brs.size()];
//		String newline=System.getProperty("line.separator");
//		String line = null;
//		while(true){
//			for(int i=0;i<lines.length;i++){
//				lines[i]=brs.get(i).readLine();
//				
//			}
//			
//			
//		}
//			
//		}
//		
//		try {
//			while ((line = br.readLine()) != null) {
//				if (line.contains("OS="))
//				s.append(line+newline);
			
//			}
		
		
	}

	public static void createResourcesFiles(Vector<StringBuilder> sv,String path,String delimeter) throws Exception {
		char delimiter = ' ';
		if (System.getProperty("os.name").equals("Linux")) {
			delimiter = '/';
		} else {
			delimiter = '\\';
		}
		
		// TODO Auto-generated method stub
		int index;
		String[] lines;
		String cpu="",net="",mem="",disk="";
		try {
			
			PrintWriter cpuwriter = new PrintWriter(path+delimiter+ delimeter+"cpu.txt", "UTF-8");
			PrintWriter memwriter = new PrintWriter(path+delimiter+ delimeter+"mem.txt", "UTF-8");
			PrintWriter netwriter = new PrintWriter(path+delimiter+ delimeter+"net.txt", "UTF-8");
			PrintWriter diskwriter = new PrintWriter(path+delimiter+ delimeter+"disk.txt", "UTF-8");
			
		
	    String newline=System.getProperty("line.separator");
		String [][]ss= new String[sv.size()][];
		String line="";
		int minl=Integer.MAX_VALUE;
		for(int i=0;i<sv.size();i++){
		ss[i]=sv.get(i).toString().split(newline);
		if(ss[i].length<minl)
			minl=ss[i].length;
		
		}
		sv.clear();
		for (int i=0;i<minl;i++){
			line="";
			for(int j=0;j<ss.length;j++)
			{
				line=line+ss[j][i]+"#";
				
			}
			lines=line.split("#");
			for (String l:lines)
			{
				cpu=cpu+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
				l=l.substring(l.indexOf(',')+1);
				mem=mem+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
				l=l.substring(l.indexOf(',')+1);
				net=net+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
				l=l.substring(l.indexOf(',')+1);
				disk=disk+l.substring(l.indexOf(':')+1,l.indexOf(','))+",";
				
				
				
			}
			cpuwriter.println(cpu);
			netwriter.println(net);
			memwriter.println(mem);
			diskwriter.println(disk);
			cpu="";
			net="";
			mem="";
			disk="";
			
			
			
		}
		cpuwriter.close();
		netwriter.close();
		memwriter.close();
		diskwriter.close();

		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	private static void readFile(File srcFile, StringBuilder s) {
		// TODO Auto-generated method stub
		
		FileInputStream fis=null;
		try {
			fis = new FileInputStream(srcFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		//Construct BufferedReader from InputStreamReader
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	    String newline=System.getProperty("line.separator");
		String line = null;
		try {
			while ((line = br.readLine()) != null) {
				if (line.contains("OS="))
				s.append(line+newline);
			
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		System.out.println("done");
	}
	
	public static void main(String args[]) {
//		createFiles("C:\\Users\\yaz\\Documents\\BG\\BGServerListenerLogs\\Exp1426103217893-nummembers1000000-numclients2-bmoderetain-threadcount50-logfalse");
	}
	
	public static void createCharts(String filespath,String delimeter) {
		// TODO Auto-generated method stub
		char delimiter = ' ';
		String pythonscriptpath;
		if (System.getProperty("os.name").equals("Linux")) {
			delimiter = '/';
			pythonscriptpath="/home/hieun/Desktop/python-plots/scripts/camp/";
		} else {
			pythonscriptpath="C:\\Users\\yaz\\Documents\\BG\\python-plots\\scripts\\camp\\";
			delimiter = '\\';
		}
		
	
		
		String cmd="python "+pythonscriptpath+"admCntrl.py "+filespath+delimiter+" "+delimeter;
		try {
			Process p=Runtime.getRuntime().exec(cmd);			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void createThroughputChart(String filespath) {
		// TODO Auto-generated method stub
		char delimiter = ' ';
		String pythonscriptpath;
		if (System.getProperty("os.name").equals("Linux")) {
			delimiter = '/';
			pythonscriptpath="/home/hieun/Desktop/python-plots/scripts/kosar/";
		} else {
			pythonscriptpath="C:\\Users\\yaz\\Documents\\BG\\python-plots\\scripts\\kosar\\";
			delimiter = '\\';
		}
		
	
		
		String cmd="python "+pythonscriptpath+"throughput.py "+filespath;
		try {
			Process p=Runtime.getRuntime().exec(cmd);			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
