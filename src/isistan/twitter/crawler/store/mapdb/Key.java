package isistan.twitter.crawler.store.mapdb;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class Key implements Comparable<Key>, Externalizable {
	byte[] k;

	public Key() {
	}

	public Key(byte[] b) {
		this.k = b;
	}

	// @Override
	// public int compareTo(Key o) {
	// return compare(k, o.k);
	// }
	//
	// public int compare(byte[] left, byte[] right) {
	// for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
	// int a = (left[i] & 0xff);
	// int b = (right[j] & 0xff);
	// if (a != b) {
	// return a - b;
	// }
	// }
	// return left.length - right.length;
	// }

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.write(k);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.k = new byte[getSize()];
		in.read(k);
	}

	protected abstract int getSize();

}