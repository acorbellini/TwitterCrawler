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
package isistan.def.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

// TODO: Auto-generated Javadoc
/**
 * The Class DEFByteBuffer.
 * 
 * @author Alejandro Corbellini
 */
public class DEFByteBuffer {

	/**
	 * The Class DEFByteArrayCache.
	 * 
	 * @author Alejandro Corbellini
	 */
	private static class DEFByteArrayCache {

		/** The usable. */
		private static HashMap<Integer, WeakReference<byte[]>[]> usable = new HashMap<>();

		/** The max. */
		private static int max = 512;

		/** The min. */
		private static int min = 32;

		/**
		 * Put.
		 * 
		 * @param notUsed
		 *            the not used
		 */
		public static void put(byte[] notUsed) {
			int size = notUsed.length;
			if (size > max || size < min)
				return;
			WeakReference<byte[]>[] list = usable.get(size);
			if (list == null)
				synchronized (usable) {
					list = usable.get(size);
					if (list == null) {
						list = new WeakReference[16];
						usable.put(size, list);
					}
				}
			synchronized (list) {
				for (int i = 0; i < list.length; i++) {
					WeakReference<byte[]> weak = list[i];
					if (weak == null || weak.get() == null) {
						list[i] = new WeakReference<byte[]>(notUsed);
						return;
					}
				}
			}

		}

		/**
		 * Gets the.
		 * 
		 * @param size
		 *            the size
		 * @return the byte[]
		 */
		public static byte[] get(int size) {
			try {
				WeakReference<byte[]>[] list = usable.get(size);
				synchronized (list) {
					for (int i = 0; i < list.length; i++) {
						WeakReference<byte[]> weak = list[i];
						if (weak != null) {
							byte[] el = weak.get();
							if (el != null) {
								list[i] = null;
								return el;
							}
						}

					}
				}
			} catch (Exception e) {
			}
			return new byte[size];
		}
	}

	/** The Constant INIT_SIZE. */
	private static final int INIT_SIZE = 8096;

	/** The buffered. */
	private byte[] buffered;

	/** The read pos. */
	private int readPos = 0;

	/** The write pos. */
	private int writePos = 0;

	/** The bos. */
	private ByteArrayOutputStream bos;

	/** The built. */
	private boolean built = false;

	/**
	 * Instantiates a new DEF byte buffer.
	 */
	public DEFByteBuffer() {
		this(INIT_SIZE);
	}

	/**
	 * Instantiates a new DEF byte buffer.
	 * 
	 * @param buffered
	 *            the buffered
	 */
	public DEFByteBuffer(byte[] buffered) {
		this(buffered, buffered.length);
	}

	/**
	 * Instantiates a new DEF byte buffer.
	 * 
	 * @param data
	 *            the data
	 * @param length
	 *            the length
	 */
	public DEFByteBuffer(byte[] data, int length) {
		this.buffered = data;
		this.writePos = length;
		this.readPos = 0;
		bos = new ByteArrayOutputStream();
	}

	/**
	 * Instantiates a new DEF byte buffer.
	 * 
	 * @param i
	 *            the i
	 */
	public DEFByteBuffer(int i) {
		this(DEFByteArrayCache.get(i), 0);
	}

	/**
	 * Gets the string.
	 * 
	 * @return the string
	 */
	public String getString() {
		int stringLength = getInt();
		if (stringLength == 0)
			return "";
		String s = new String(buffered, readPos, stringLength);
		readPos += stringLength;
		return s;
	}

	/**
	 * Gets the int.
	 * 
	 * @return the int
	 */
	public int getInt() {
		int val = DataTypeUtils.byteArrayToInt(buffered, readPos);
		readPos += 4;
		return val;
	}

	/**
	 * Gets the long.
	 * 
	 * @return the long
	 */
	public long getLong() {
		long val = DataTypeUtils.byteArrayToLong(buffered, readPos);
		readPos += 8;
		return val;

	}

	/**
	 * Sets the offset.
	 * 
	 * @param off
	 *            the new offset
	 */
	public void setOffset(int off) {
		this.readPos = off;
	}

	/**
	 * Gets the boolean.
	 * 
	 * @return the boolean
	 */
	public boolean getBoolean() {
		boolean val = (buffered[readPos] & 0xF) == 0xF;
		readPos++;
		return val;
	}

	/**
	 * Gets the byte array.
	 * 
	 * @return the byte array
	 */
	public byte[] getByteArray() {
		int length = getInt();
		return get(length);
	}

	/**
	 * Gets the.
	 * 
	 * @return the byte
	 */
	public byte get() {
		byte val = buffered[readPos];
		readPos++;
		return val;
	}

	/**
	 * Gets the offset.
	 * 
	 * @return the offset
	 */
	public int getOffset() {
		return readPos;
	}

	/**
	 * Gets the.
	 * 
	 * @param length
	 *            the length
	 * @return the byte[]
	 */
	public byte[] get(int length) {
		byte[] val = Arrays.copyOfRange(buffered, readPos, readPos + length);
		readPos += length;
		return val;
	}

	/**
	 * Gets the uuid.
	 * 
	 * @return the uuid
	 */
	public UUID getUUID() {
		return new UUID(getLong(), getLong());
	}

	/**
	 * Gets the float.
	 * 
	 * @return the float
	 */
	public float getFloat() {
		ByteBuffer buff = ByteBuffer.wrap(buffered, readPos, 4);
		readPos += 4;
		return buff.getFloat();
	}

	/**
	 * Gets the short byte array.
	 * 
	 * @return the short byte array
	 */
	public byte[] getShortByteArray() {
		byte l = get();
		return get(l);
	}

	/**
	 * Gets the sets the.
	 * 
	 * @return the sets the
	 */
	public Set<String> getSet() {
		Set<String> ret = new HashSet<>();
		int num = getInt();
		for (int i = 0; i < num; i++)
			ret.add(getString());
		return ret;
	}

	/**
	 * Checks for remaining.
	 * 
	 * @return true, if successful
	 */
	public boolean hasRemaining() {
		return readPos < writePos;
	}

	/**
	 * Gets the raw byte array.
	 * 
	 * @return the raw byte array
	 */
	public byte[] getRawByteArray() {
		return Arrays.copyOfRange(buffered, readPos, writePos);
	}

	/**
	 * Size.
	 * 
	 * @return the int
	 */
	public int size() {
		return writePos;
	}

	/**
	 * Gets the map.
	 * 
	 * @return the map
	 */
	public Map<String, String> getMap() {
		int size = getInt();
		Map<String, String> ret = new HashMap<>();
		for (int i = 0; i < size; i++) {
			ret.put(getString(), getString());
		}
		return ret;
	}

	/**
	 * Put range.
	 * 
	 * @param original
	 *            the original
	 * @param originalOffset
	 *            the original offset
	 * @param length
	 *            the length
	 */
	public void putRange(byte[] original, int originalOffset, int length) {
		putByteArray(Arrays.copyOfRange(original, originalOffset,
				originalOffset + length));
	}

	/**
	 * Put boolean.
	 * 
	 * @param b
	 *            the b
	 */
	public void putBoolean(boolean b) {
		put((byte) (b ? 0xF : 0x0));
	}

	/**
	 * Ensure capacity.
	 * 
	 * @param i
	 *            the i
	 */
	private void ensureCapacity(int i) {
		if (built)
			System.out.println("CANNOT WRITE AFTER BUFFER IS BUILT.");
		while (writePos + i > buffered.length) {
			byte[] copy = buffered;
			byte[] bufferedExtended = DEFByteArrayCache
					.get(buffered.length == 0 ? INIT_SIZE : buffered.length * 2);
			System.arraycopy(buffered, 0, bufferedExtended, 0, buffered.length);
			buffered = bufferedExtended;
			DEFByteArrayCache.put(copy);
		}

	}

	/**
	 * Put long.
	 * 
	 * @param l
	 *            the l
	 */
	public void putLong(long l) {
		putRawByteArray(DataTypeUtils.longToByteArray(l));
	}

	/**
	 * Put byte array.
	 * 
	 * @param data
	 *            the data
	 */
	public void putByteArray(byte[] data) {
		putInt(data.length);
		putRawByteArray(data);
	}

	/**
	 * Put object.
	 * 
	 * @param o
	 *            the o
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void putObject(Object o) throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(o);
		oos.reset();
		byte[] ba = bos.toByteArray();
		bos.reset();

		putByteArray(ba);
	}

	/**
	 * Put uuid.
	 * 
	 * @param jobID
	 *            the job id
	 */
	public void putUUID(UUID jobID) {
		putLong(jobID.getMostSignificantBits());
		putLong(jobID.getLeastSignificantBits());
	}

	/**
	 * Put set.
	 * 
	 * @param tags
	 *            the tags
	 */
	public void putSet(Set<String> tags) {
		putInt(tags.size());
		for (String string : tags)
			putString(string);
	}

	/**
	 * Put short byte array.
	 * 
	 * @param build
	 *            the build
	 */
	public void putShortByteArray(byte[] build) {
		put((byte) build.length);
		putRawByteArray(build);
	}

	/**
	 * Put float.
	 * 
	 * @param v
	 *            the v
	 */
	public void putFloat(float v) {
		byte[] asbytes = ByteBuffer.allocate(4).putFloat(v).array();
		putRawByteArray(asbytes);
	}

	/**
	 * Put front.
	 * 
	 * @param build
	 *            the build
	 */
	public void putFront(byte[] build) {
		ensureCapacity(build.length);
		System.arraycopy(buffered, 0, buffered, build.length, writePos);
		writePos += build.length;
		System.arraycopy(build, 0, buffered, 0, build.length);
	}

	/**
	 * Put long front.
	 * 
	 * @param l
	 *            the l
	 */
	private void putLongFront(long l) {
		putFront(DataTypeUtils.longToByteArray(l));
	}

	/**
	 * Put.
	 * 
	 * @param val
	 *            the val
	 */
	public void put(Byte val) {
		ensureCapacity(1);
		buffered[writePos] = val;
		writePos++;
	}

	/**
	 * Put raw byte array.
	 * 
	 * @param data
	 *            the data
	 */
	public void putRawByteArray(byte[] data) {
		ensureCapacity(data.length);
		System.arraycopy(data, 0, buffered, writePos, data.length);
		writePos += data.length;
	}

	/**
	 * Put string.
	 * 
	 * @param s
	 *            the s
	 */
	public void putString(String s) {
		if (s == null || s.isEmpty()) {
			putInt(0);
			return;
		}

		byte[] stringAsBytes = s.getBytes();
		putInt(stringAsBytes.length);
		putRawByteArray(stringAsBytes);
	}

	/**
	 * Put int.
	 * 
	 * @param i
	 *            the i
	 */
	public void putInt(int i) {
		putRawByteArray(DataTypeUtils.intToByteArray(i));
	}

	/**
	 * Builds the.
	 * 
	 * @return the byte[]
	 */
	public byte[] build() {
		built = true;
		byte[] ret = Arrays.copyOf(buffered, writePos);
		DEFByteArrayCache.put(buffered);
		return ret;
	}

	/**
	 * Put uuid front.
	 * 
	 * @param id
	 *            the id
	 */
	public void putUUIDFront(UUID id) {
		putLongFront(id.getLeastSignificantBits());
		putLongFront(id.getMostSignificantBits());
	}

	/**
	 * Clear.
	 */
	public void clear() {
		buffered = new byte[INIT_SIZE];
		writePos = 0;
		readPos = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return Arrays.toString(buffered);
	}

	/**
	 * Put map.
	 * 
	 * @param data
	 *            the data
	 */
	public void putMap(Map<String, String> data) {
		putInt(data.size());
		for (Entry<String, String> e : data.entrySet()) {
			putString(e.getKey());
			putString(e.getValue());
		}
	}
}
