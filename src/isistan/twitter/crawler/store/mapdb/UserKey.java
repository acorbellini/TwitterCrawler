package isistan.twitter.crawler.store.mapdb;

import isistan.def.utils.DEFByteBuffer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class UserKey implements Externalizable, Comparable<UserKey> {
	long uid;

	public UserKey() {
		// TODO Auto-generated constructor stub
	}

	public UserKey(long uid) {
		this.uid = uid;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		DEFByteBuffer buff = new DEFByteBuffer();
		buff.putLong(uid);
		out.write(buff.build());

	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		byte[] data = new byte[8];
		in.read(data);
		DEFByteBuffer buff = new DEFByteBuffer(data);
		this.uid = buff.getLong();
	}

	@Override
	public int compareTo(UserKey o) {
		return Long.compare(uid, o.uid);
	}

}
