/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
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

package kosar;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Utility functions.
 */
public class Utils {
	private static final Random rand = new Random();
	private static final ThreadLocal<Random> rng = new ThreadLocal<Random>();

	public static Random random() {
		Random ret = rng.get();
		if (ret == null) {
			ret = new Random(rand.nextLong());
			rng.set(ret);
		}
		return ret;
	}

	/**
	 * Generate a random ASCII string of a given length.
	 */
	public static String ASCIIString(int length) {
		int interval = '~' - ' ' + 1;

		byte[] buf = new byte[length];
		random().nextBytes(buf);
		for (int i = 0; i < length; i++) {
			if (buf[i] < 0) {
				buf[i] = (byte) ((-buf[i] % interval) + ' ');
			} else {
				buf[i] = (byte) ((buf[i] % interval) + ' ');
			}
		}
		return new String(buf);
	}

	/**
	 * Hash an integer value.
	 */
	public static long hash(long val) {
		return FNVhash64(val);
	}

	public static final int FNV_offset_basis_32 = 0x811c9dc5;
	public static final int FNV_prime_32 = 16777619;

	/**
	 * 32 bit FNV hash. Produces more "random" hashes than (say)
	 * String.hashCode().
	 * 
	 * @param val
	 *            The value to hash.
	 * @return The hash value
	 */
	public static int FNVhash32(int val) {
		// from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
		int hashval = FNV_offset_basis_32;

		for (int i = 0; i < 4; i++) {
			int octet = val & 0x00ff;
			val = val >> 8;

			hashval = hashval ^ octet;
			hashval = hashval * FNV_prime_32;
			// hashval = hashval ^ octet;
		}
		return Math.abs(hashval);
	}

	public static final long FNV_offset_basis_64 = 0xCBF29CE484222325L;
	public static final long FNV_prime_64 = 1099511628211L;

	/**
	 * 64 bit FNV hash. Produces more "random" hashes than (say)
	 * String.hashCode().
	 * 
	 * @param val
	 *            The value to hash.
	 * @return The hash value
	 */
	public static long FNVhash64(long val) {
		// from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
		long hashval = FNV_offset_basis_64;

		for (int i = 0; i < 8; i++) {
			long octet = val & 0x00ff;
			val = val >> 8;

			hashval = hashval ^ octet;
			hashval = hashval * FNV_prime_64;
			// hashval = hashval ^ octet;
		}
		return Math.abs(hashval);
	}

	public static <K, V> void printMap(Map<K, V> map) {
		Set<K> keys = map.keySet();
		for (K key : keys) {
			System.out.println(key.toString() + ":" + map.get(key));
		}
	}

	public static String genereateUUID() {
		return UUID.randomUUID().toString().replaceAll("-", "");
	}

	public static long genID(int port) {
		String ipaddr = null;
		try {
			ipaddr = getLocalHostLANAddress().getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out
					.println("Error: Cannot get local IP address. GenID failed.");
			System.exit(-1);
		}

		// Parse IP parts into an int array
		int[] ip = new int[4];
		String[] parts = ipaddr.split("\\.");

		for (int i = 0; i < 4; i++) {
			ip[i] = Integer.parseInt(parts[i]);
		}

		// Add the above IP parts into an int number representing your IP
		// in a 32-bit binary form
		long ipNumber = 0;
		for (int i = 0; i < 4; i++) {
			ipNumber += ip[i] << (24 - (8 * i));
		}

		ipNumber = ipNumber << 32;

		return ipNumber + (long) port;
	}

	/**
	 * Returns an <code>InetAddress</code> object encapsulating what is most
	 * likely the machine's LAN IP address.
	 * <p/>
	 * This method is intended for use as a replacement of JDK method
	 * <code>InetAddress.getLocalHost</code>, because that method is ambiguous
	 * on Linux systems. Linux systems enumerate the loopback network interface
	 * the same way as regular LAN network interfaces, but the JDK
	 * <code>InetAddress.getLocalHost</code> method does not specify the
	 * algorithm used to select the address returned under such circumstances,
	 * and will often return the loopback address, which is not valid for
	 * network communication. Details <a
	 * href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4665037"
	 * >here</a>.
	 * <p/>
	 * This method will scan all IP addresses on all network interfaces on the
	 * host machine to determine the IP address most likely to be the machine's
	 * LAN address. If the machine has multiple IP addresses, this method will
	 * prefer a site-local IP address (e.g. 192.168.x.x or 10.10.x.x, usually
	 * IPv4) if the machine has one (and will return the first site-local
	 * address if the machine has more than one), but if the machine does not
	 * hold a site-local address, this method will return simply the first
	 * non-loopback address found (IPv4 or IPv6).
	 * <p/>
	 * If this method cannot find a non-loopback address using this selection
	 * algorithm, it will fall back to calling and returning the result of JDK
	 * method <code>InetAddress.getLocalHost</code>.
	 * <p/>
	 *
	 * @throws UnknownHostException
	 *             If the LAN address of the machine cannot be found.
	 */
	private static InetAddress getLocalHostLANAddress()
			throws UnknownHostException {
		try {
			InetAddress candidateAddress = null;
			// Iterate all NICs (network interface cards)...
			for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces
					.hasMoreElements();) {
				NetworkInterface iface = (NetworkInterface) ifaces
						.nextElement();
				// Iterate all IP addresses assigned to each card...
				for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs
						.hasMoreElements();) {
					InetAddress inetAddr = (InetAddress) inetAddrs
							.nextElement();
					if (!inetAddr.isLoopbackAddress()) {

						if (inetAddr.isSiteLocalAddress()) {
							// Found non-loopback site-local address. Return it
							// immediately...
							return inetAddr;
						} else if (candidateAddress == null) {
							// Found non-loopback address, but not necessarily
							// site-local.
							// Store it as a candidate to be returned if
							// site-local address is not subsequently found...
							candidateAddress = inetAddr;
							// Note that we don't repeatedly assign non-loopback
							// non-site-local addresses as candidates,
							// only the first. For subsequent iterations,
							// candidate will be non-null.
						}
					}
				}
			}
			if (candidateAddress != null) {
				// We did not find a site-local address, but we found some other
				// non-loopback address.
				// Server might have a non-site-local address assigned to its
				// NIC (or it might be running
				// IPv6 which deprecates the "site-local" concept).
				// Return this non-loopback candidate address...
				return candidateAddress;
			}
			// At this point, we did not find a non-loopback address.
			// Fall back to returning whatever InetAddress.getLocalHost()
			// returns...
			InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
			if (jdkSuppliedAddress == null) {
				throw new UnknownHostException(
						"The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
			}
			return jdkSuppliedAddress;
		} catch (Exception e) {
			UnknownHostException unknownHostException = new UnknownHostException(
					"Failed to determine LAN address: " + e);
			unknownHostException.initCause(e);
			throw unknownHostException;
		}
	}

	public static void main(String[] args) {
		System.out.println(Utils.genID(5000));
	}
}
