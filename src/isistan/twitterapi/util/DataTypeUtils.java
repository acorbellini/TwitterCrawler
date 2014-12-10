/**
 * Copyright 2014 Alejandro Corbellini
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *  
 * 
 */
package isistan.twitterapi.util;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

// TODO: Auto-generated Javadoc
/**
 * The Class DataTypeUtils.
 *
 * @author Alejandro Corbellini
 */
public class DataTypeUtils {

	/**
	 * Byte array to int.
	 *
	 * @param b
	 *            the b
	 * @param init
	 *            the init
	 * @return the int
	 */
	public static int byteArrayToInt(byte[] b, int init) {
		return b[init + 3] & 0xFF | (b[init + 2] & 0xFF) << 8
				| (b[init + 1] & 0xFF) << 16 | (b[init + 0] & 0xFF) << 24;
	}

	/**
	 * Byte array to int.
	 *
	 * @param b
	 *            the b
	 * @return the int
	 */
	public static int byteArrayToInt(byte[] b) {
		return byteArrayToInt(b, 0);
	}

	/**
	 * Byte array to int array.
	 *
	 * @param data
	 *            the data
	 * @return the int[]
	 */
	public static int[] byteArrayToIntArray(byte[] data) {
		int[] ret = new int[data.length / 4];
		for (int i = 0; i < (data.length / 4); i++) {
			byte[] d = new byte[] { data[i * 4 + 0], data[i * 4 + 1],
					data[i * 4 + 2], data[i * 4 + 3] };
			ret[i] = byteArrayToInt(d);
		}
		return ret;
	}

	/**
	 * Int array to byte array.
	 *
	 * @param data
	 *            the data
	 * @return the byte[]
	 */
	public static byte[] intArrayToByteArray(int[] data) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(data.length * 4);
		IntBuffer intBuffer = byteBuffer.asIntBuffer();
		intBuffer.put(data);
		return byteBuffer.array();
	}

	/**
	 * Int to byte array.
	 *
	 * @param a
	 *            the a
	 * @return the byte[]
	 */
	public static byte[] intToByteArray(int a) {
		byte[] ret = new byte[4];
		ret[3] = (byte) (a & 0xFF);
		ret[2] = (byte) ((a >> 8) & 0xFF);
		ret[1] = (byte) ((a >> 16) & 0xFF);
		ret[0] = (byte) ((a >> 24) & 0xFF);
		return ret;
	}

	/**
	 * Long to byte array.
	 *
	 * @param val
	 *            the val
	 * @return the byte[]
	 */
	public static byte[] longToByteArray(long val) {
		byte[] b = new byte[8];
		for (int i = 7; i > 0; i--) {
			b[i] = (byte) val;
			val >>>= 8;
		}
		b[0] = (byte) val;
		return b;
	}

	/**
	 * Byte array to long.
	 *
	 * @param bytes
	 *            the bytes
	 * @param offset
	 *            the offset
	 * @return the long
	 */
	public static long byteArrayToLong(byte[] bytes, int offset) {
		long l = 0;
		for (int i = offset; i < offset + 8; i++) {
			l <<= 8;
			l ^= bytes[i] & 0xFF;
		}
		return l;
	}

}
