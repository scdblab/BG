/**
 * Authors:  Aniruddh Munde and Snehal Deshmukh
 */
package edu.usc.bg;

import java.util.concurrent.Semaphore;

public class Server {

	private int numServers;
	private static Semaphore S;		  
	static int serviceTime = 1; //milliseconds

	Server(int InputnumServers, int Inputservicetime) {
		numServers = InputnumServers;
		//Initialize the semaphore to numServers as its value
		serviceTime = Inputservicetime;
		S = new Semaphore(numServers);
	}

	public static void service(){
		try {
			//Wait on S
			S.acquire();
			System.out.println("------- request going to be processed");
			Thread.sleep(serviceTime);
			System.out.println("------- after processing request");

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		//Release S
		S.release();	
	}
}