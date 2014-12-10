package isistan.twitter.crawler.store.mapdb;

import isistan.def.utils.DEFByteBuffer;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class TweetKey implements Externalizable, Comparable<TweetKey> {
	long uid;
	long tid;

	public TweetKey() {
		// TODO Auto-generated constructor stub
	}

	public TweetKey(long uid, long tid) {
		this.uid = uid;
		this.tid = tid;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		DEFByteBuffer buff = new DEFByteBuffer();
		buff.putLong(uid);
		buff.putLong(tid);
		out.write(buff.build());

	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		byte[] data = new byte[16];
		in.read(data);
		DEFByteBuffer buff = new DEFByteBuffer(data);
		this.uid = buff.getLong();
		this.tid = buff.getLong();
	}

	@Override
	public int compareTo(TweetKey o) {
		int ucomp = Long.compare(uid, o.uid);
		if (ucomp == 0)
			return Long.compare(tid, o.tid);
		return ucomp;
	}

}
