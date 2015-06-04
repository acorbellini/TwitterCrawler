package isistan.twitter.crawler.tweet;

import java.io.Serializable;

import edu.jlime.util.ByteBuffer;

public class Reply implements Serializable {

	public long inreplytouid = -1;
	public long inreplytostatusid = -1;
	public String inreplytoscn = "";

	public Reply(String inReplyToScreenName, long inReplyToUserId,
			long inReplyToStatusId) {
		this.inreplytouid = inReplyToUserId;
		this.inreplytostatusid = inReplyToStatusId;
		this.inreplytoscn = inReplyToScreenName;
	}

	public Reply() {
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Reply other = (Reply) obj;
		if (inreplytouid != other.inreplytouid)
			return false;
		if (inreplytoscn == null) {
			if (other.inreplytoscn != null)
				return false;
		} else if (!inreplytoscn.equals(other.inreplytoscn))
			return false;
		if (inreplytostatusid != other.inreplytostatusid)
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (inreplytouid ^ (inreplytouid >>> 32));
		result = prime * result
				+ ((inreplytoscn == null) ? 0 : inreplytoscn.hashCode());
		result = prime * result
				+ (int) (inreplytostatusid ^ (inreplytostatusid >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "Reply [inreplytoid=" + inreplytouid + ", inreplytostatusid="
				+ inreplytostatusid + ", inreplytoscn=" + inreplytoscn + "]";
	}

	public void writeTo(ByteBuffer buffer) {
		if (inreplytouid == -1) {
			buffer.put((byte) 0);
		} else {
			buffer.put((byte) 1);
			buffer.putLong(inreplytouid);
			buffer.putLong(inreplytostatusid);
			buffer.putString(inreplytoscn);
		}
	}

	public Reply readFrom(ByteBuffer buffer) {
		byte type = buffer.get();
		if (type == 1) {
			inreplytouid = buffer.getLong();
			inreplytostatusid = buffer.getLong();
			inreplytoscn = buffer.getString();
		}
		return this;
	}

}
