package edu.usc.bg.server;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;



public class KVSClientRegistrar {}
/*
	
	public static final String CHARACTERS = new String("abcdefghijklmnopqrstuvwxyz1234567890_ABCDEFGHIJLKMNOPQRSTUVWXYZ");

	/*public static byte[] register(SocketIO socket, byte[] request, boolean initConnect) {
		/*
		 * Content of request:
		 * int:ipAddress.length
		 * byte[]:ipAddress
		 * int:numPorts
		 * byte[]:Ports
		 * int:Listening Port
		 
		int index = 0;
		int ipAddressLength = ByteBuffer.wrap(Arrays.copyOfRange(request, index, index+4)).getInt();
		index += 4;
		byte[] ipAddress = Arrays.copyOfRange(request, index, index+ipAddressLength);
		index += ipAddressLength;
		
		ipAddressLength--; //one fewer period than digits.
		StringBuilder sb = new StringBuilder();
		for(byte b : ipAddress) {
			sb.append(b & 0xFF);
			if(--ipAddressLength >= 0) sb.append('.');
		}
		String ip = sb.toString();
		
		int[] portList = new int[ByteBuffer.wrap(Arrays.copyOfRange(request, index, index+4)).getInt()];
		index+=4;
		for(int i = 0; i < portList.length; i++) {
			portList[i] = ByteBuffer.wrap(Arrays.copyOfRange(request, index, index+4)).getInt();
			index+=4;
		}
		
		int invalidationPort = ByteBuffer.wrap(Arrays.copyOfRange(request, index, index+4)).getInt();
		System.out.println("Client Invalidation Port: " + invalidationPort);
		//KosarCore.invalidationPorts.put(ip, invalidationPort);
		
		try {
			Socket sock = new Socket(ip, invalidationPort);
			System.out.println("Connected to Client Invalidation");
			SocketIO sockIO = new SocketIO(sock);
			///KosarCore.invalidationSockets.add(sockIO);
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		//Generates a new 4-byte client id, and puts it in the KosarCore client data structure
		//byte[] id = generateNewClientID(socket, portList);
		
		System.out.println("Successful connection to Client Invalidation Port " + invalidationPort);
		//Communicates back to the KVS Client.
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write(3);
			baos.write(id);
			ByteBuffer bb = ByteBuffer.allocate(4);
			//bb.putInt(KosarCore.getNumReplicas());
			baos.write(bb.array());
			baos.flush();
			socket.writeBytes(baos.toByteArray());
		} catch (IOException e) {
			System.out.println("IOException:KVSClientRegistrar - Failed to send Client connection acknowledgment.");
			e.printStackTrace();
		}
		System.out.print("Client Registered: ");
		for(byte i : id) System.out.print(i + " ");
		System.out.println("");
		
		return id;
	}*/
	
	
	
/*
	public static void main(String args[]) {
		//KosarCore.clientToPortsMap.put(ByteBuffer.wrap(new byte[]{68,33,22,11}),new byte[]{0});
	}
}
*/