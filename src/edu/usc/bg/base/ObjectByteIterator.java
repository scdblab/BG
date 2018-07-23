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

package edu.usc.bg.base;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ObjectByteIterator extends ByteIterator implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6820788153816590863L;

	//String str;
	int off;
	byte[] bytes;
	
	public ObjectByteIterator copy() {
		byte[] bs = Arrays.copyOf(bytes, bytes.length);
		ObjectByteIterator obj = new ObjectByteIterator(bs);
		return obj;
	}
	
	public static void deepCopy(Map<String, ByteIterator> original, Map<String, ByteIterator> dest) {
		if (original == null) {
			return;
		}
		Set<String> keys = original.keySet();
		for (String key : keys) {
			ByteIterator value = original.get(key);
			if (ObjectByteIterator.class.isInstance(value)) {
				ObjectByteIterator cast = ObjectByteIterator.class.cast(value);
				dest.put(key, cast.copy());
			}
		}
	}


	/**
	 * Put all of the entries of one map into the other, converting
	 * String values into ByteIterators.
	 */
	public static void putAllAsByteIterators(Map<String, ByteIterator> out, Map<String, String> in) {
	       for(String s: in.keySet()) { out.put(s, new ObjectByteIterator(in.get(s).getBytes())); }
	} 

	/**
	 * Put all of the entries of one map into the other, converting
	 * ByteIterator values into Strings.
	 */
	public static void putAllAsStrings(Map<String, String> out, Map<String, ByteIterator> in) {
	       for(String s: in.keySet()) { out.put(s, in.get(s).toString()); }
	} 

	/**
	 * Create a copy of a map, converting the values from Strings to
	 * StringByteIterators.
	 */
	public static HashMap<String, ByteIterator> getByteIteratorMap(Map<String, String> m) {
		HashMap<String, ByteIterator> ret =
			new HashMap<String,ByteIterator>();

		for(String s: m.keySet()) {
			ret.put(s, new ObjectByteIterator(m.get(s).getBytes()));
		}
		return ret;
	}

	/**
	 * Create a copy of a map, converting the values from
	 * StringByteIterators to Strings.
	 */
	public static HashMap<String, String> getStringMap(Map<String, ByteIterator> m) {
		HashMap<String, String> ret = new HashMap<String,String>();

		for(String s: m.keySet()) {
			ret.put(s, m.get(s).toString());;
		}
		return ret;
	}

	/*public ObjectByteIterator(String s) {
		this.str = s;
		this.off = 0;
	}*/
	
	public ObjectByteIterator(byte[] s) {
		this.bytes = s;
		this.off = 0;
	}
	@Override
	public boolean hasNext() {
		return off < bytes.length;
		//return off < str.length();
	}

	@Override
	public byte nextByte() {
		//byte ret = (byte)str.charAt(off);
		byte ret = bytes[off];
		off++;
		return ret;
	}

	@Override
	public long bytesLeft() {
		return bytes.length - off;
		//return str.length() - off;
	}

	/**
	 * Specialization of general purpose toString() to avoid unnecessary
	 * copies.
	 * <p>
	 * Creating a new StringByteIterator, then calling toString()
	 * yields the original String object, and does not perform any copies
	 * or String conversion operations.
	 * </p>
	 */
	@Override
	public String toString() {
		String newString = new String(bytes);
		return newString;
	}
	
	@Override
	public int resetOffset() {
		int offset = off;
		off = 0;
		return offset;
	}
}
