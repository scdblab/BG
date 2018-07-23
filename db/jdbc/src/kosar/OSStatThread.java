package kosar;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class OSStatThread extends Thread {
	String cmd = "typeperf \"\\Memory\\Available bytes\" \"\\processor(_total)\\% processor time\" \"\\PhysicalDisk(_Total)\\Avg. Disk Write Queue Length\" \"\\Network Interface(*)\\Bytes Total/sec\" -sc 1";
	boolean end = false;
	double availableMem = 0.0;
	double cpuTime = 0.0;
	double avgQLength = 0.0;
	double netwrokBytesPerSec = 0.0;

	public void setEnd() {
		end = true;
	}

	public void run() {
		while (!end) {
			this.monitor();
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void monitor() {
		try {
			int cnt = 0;
			Process osStats = Runtime.getRuntime().exec(cmd);
			InputStream stdout = osStats.getInputStream();
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(stdout));
			String osline = "";
			while ((osline = reader.readLine()) != null) {

				cnt++;

				// parse the line and send to coord
				if (cnt == 3) {
					String memory, cpu, network, disk;
					String[] stats = osline.split(",");
					double mem = Double.parseDouble(stats[1].substring(
							stats[1].indexOf("\"") + 1,
							stats[1].lastIndexOf("\"")));
					mem = mem / (1024 * 1024);
					memory = "Available MEM(MB):" + mem + ",";
					cpu = "CPU:"
							+ stats[2].substring(
									stats[2].indexOf("\"") + 1,
									stats[2].lastIndexOf("\"")) + ",";
					disk = "DISK:"
							+ stats[3].substring(
									stats[3].indexOf("\"") + 1,
									stats[3].lastIndexOf("\"")) + ",";
					double net = Double.parseDouble(stats[4].substring(
							stats[4].indexOf("\"") + 1,
							stats[4].lastIndexOf("\"")));
					net = net / (1024 * 1024);
					network = "NTBW(MB/sec):" + net + ",";
					String line = "OS=" + cpu + memory + network + disk;
					System.out.println(line);
				}
			}
			osStats.waitFor();
			if (osStats != null)
				osStats.destroy();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		OSStatThread statThread = new OSStatThread();
		statThread.monitor();
	}

}
