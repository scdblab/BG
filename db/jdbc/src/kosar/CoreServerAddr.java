package kosar;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

public class CoreServerAddr {

	public static final int port = 8888;

	private static String SERVER_IP;
	
	public static String[] shards = new String[]{
//		"10.0.1.25:8888",
//		"10.0.1.20:8888","10.0.1.15:8888","10.0.1.10:8888",
		"10.0.0.220:8888",
	};
	
	public static String[] dmlProcessAddrs = new String[] {
//		"10.0.1.25:18188",
//		"10.0.1.20:18188","10.0.1.15:18188","10.0.1.10:18188",
		"10.0.0.220:18188"
	};
	
	private static final boolean twoLevelhashing = false;

	public static synchronized final String getServerIP() {
		if (SERVER_IP == null) {
			try {
				SERVER_IP = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		return SERVER_IP;
	}

	public static final String getServerAddr() {
		return getServerIP() + ":" + port;
	}
	
	public static final String getServerAddr(int inviter, int invitee) {
		int index = (inviter + invitee) % shards.length;
		return shards[index];
	}

	public static void setServerAddr(String[] coreaddr) {
		shards = coreaddr;
	}

	public static String getServerAddr(String ik) {
		int id = -1;
		
		if (twoLevelhashing) {
			String str = ik.replaceAll("[^-0-9]+", " ");
			List<String> list = Arrays.asList(str.trim().split(" "));
			id = Integer.parseInt(list.get(0));
		} else {
			id = ik.hashCode();
			id = id < 0 ? -id : id;
		}
		
		return shards[id % shards.length];
	}

	public static String getDMLProcessAddr(int inviter, int invitee) {
		int index = (inviter+invitee) % dmlProcessAddrs.length;
		return dmlProcessAddrs[index];
	}
}
