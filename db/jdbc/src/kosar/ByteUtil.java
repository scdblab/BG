package kosar;

import java.io.UnsupportedEncodingException;

/**
 * A utility class that convert byte array to string and vice versa 
 * 
 * @author Haoyu
 *
 */
public class ByteUtil {
	
	public static final String UTF8 = "UTF-8";
	
	/**
	 * convert byte array to string
	 * 
	 * @param bytes
	 * @return converted string
	 */
	public static String byte2String(byte[] bytes) {
		try {
			return new String(bytes, UTF8);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * convert string into byte array
	 * 
	 * @param text
	 * @return converted byte array
	 */
	public static byte[] string2Byte(String text) {
		try {
			return text.getBytes(UTF8);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}
}
